#
# Interface with works
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import os
import sys
import requests
import csv
import traceback
from crossref.restful import Works
from tdm_base import TdmBase
from tdm_payload import TdmPayload
from xml.etree import ElementTree
from tdm_html import TdmHtml

class TdmWorks(TdmBase):
    def __init__(self):
        self.works = Works()
        self.total_results_DOI = 0
        self.TOTAL = 0
        self.TOTAL_PERC  = 1
        self.PDF = 2
        self.HTML = 3
        self.HTML_META = 4
        self.XML = 5
        self.TXT = 6
        self.UNK = 7
        self.FAIL = "FAIL"
        self.HTML_META_EXT = "HTML_META"
        self.statsDOIGroup ={}   #Dictionary that keeps the count of file per type
        self.tdm_html = TdmHtml()
        super().__init__()




    def partial_filter_by_issn(self, issn, rows=20, offset=0):
        irows = int(rows)
        ioffset = int(offset)
        srows = str(irows)
        soffset = str(ioffset)
        sissn = str(issn)
        api_url = self.url_prefix + '/works?filter=issn:' + sissn + '&rows=' + srows + '&offset=' + soffset
        rest_api_result = self.get(api_url)
        result = TdmPayload(rest_api_result.content.decode("utf-8"))
        return result

    def filter_by_issn(self, issn, rows=20, offset=0):
        result = self.partial_filter_by_issn(issn)
        total_results = result.message.get('total-results')
        self.total_results_DOI = total_results
        yield result
        current_offset = offset + 20
        while current_offset < total_results:
            yield self.partial_filter_by_issn(issn, rows=20, offset=current_offset)
            current_offset = current_offset + 20

    #Given a list of filenames per doi which can include repeated files
    #Returns how many files per doi are, one file per type only.
    def count_filetypes_by_doi(self, doi, doiFileNameList):
      try:
        doiFileNameProcessed = []
        full = 0
        metadata = 0
        failed = 0
        info = []
        for fn in doiFileNameList:
            if "#" in fn:
                fn_as_a_list = fn.split('#')
                if len(fn_as_a_list) >= 3:
                    fn = fn_as_a_list[0]
                    if fn not in doiFileNameProcessed:
                        doiFileNameProcessed.append(fn)
                        extension = fn_as_a_list[1]
                        url = fn_as_a_list[2]
                        if extension != self.FAIL:
                            if extension == "pdf":
                                t = self.PDF
                                full = full + 1
                            elif extension == "html":
                                t = self.HTML
                                full = full + 1
                            elif extension == "xml":
                                t = self.XML
                                full = full + 1
                            elif extension == "txt":
                                t = self.TXT
                                full = full + 1
                            elif extension == self.HTML_META_EXT:
                                t = self.HTML_META
                                metadata = metadata+1
                            else:
                                t = self.UNK
                            doi_split = doi.split('/')
                            doigroup = doi_split[0]
                            if not doigroup in self.statsDOIGroup:
                                self.statsDOIGroup[doigroup] = {self.TOTAL:" " , self.TOTAL_PERC: " ", self.PDF: 0, self.HTML: 0,  self.HTML_META: 0, self.XML: 0, self.TXT: 0,
                                                        self.UNK: 0}
                            self.statsDOIGroup[doigroup][t] = self.statsDOIGroup[doigroup][t] + 1

                        else:
                            failed = failed + 1

                        #doiFilenameList.append(fn + "\t")
                        info.append(fn)
                        info.append(extension)   #To be recorded by file_nameLDois
                        info.append(url)
      except:
          print("Unexpected error at count_filetypes_by_doi:", sys.exc_info()[0])
          traceback.print_exc()
      return (info, full, metadata, failed)



    def download_by_issn(self, root_dir, issn, number):
        try:
            self.create_dir(root_dir)

            file_name = root_dir + "/" + issn + "_alllinks.txt"
            self.fall = open(file_name, "w+")
            self.fall.write("DOI \t LINK \t CONTENT-TYPE \t EXTENSION \n")

            file_nameS = root_dir + "/" + issn + "_success.txt"       #Records all the links that were sucessfully retrieved.  A Doi can have more than one link to download.
            self.fsuc = open(file_nameS, "w+")
            self.fsuc.write("DOI \t LINK \n")
            file_nameU = root_dir + "/" + issn + "_issues.txt"        #Records all the links that failed to be retrieved.
            self.funsuc = open(file_nameU, "w+")
            self.funsuc.write("DOI \t LINK \t RESPONSE CODE \n")


            file_nameLDois = root_dir + "/" + issn + "_DOIsTrack.csv" #Records a list of all the DOIs processed by the application and its corresponding donwload files.
            csvfileLD = open(file_nameLDois, 'w', newline='')
            self.LDWriter = csv.writer(csvfileLD, delimiter=',')
            self.LDWriter.writerow(['NUM', 'DOI', 'LOCAL FILE', 'EXTENSION', 'URL'])


            #A URL is faulty if the request responds <200 or >300
            #or if the received content is different from the expected content
            #Write a list of faulty URLs
            file_nameRetry = root_dir + "/" + issn + "_FaultyURLS.csv"
            csvfile=open(file_nameRetry, 'w', newline='')
            self.retryWriter = csv.writer(csvfile, delimiter=',')
            self.retryWriter.writerow(['DOI', 'URL', 'RESPONSE CODE', 'REASON'])

            self.total_results_DOI = 0           #Total resuls returned by Crossref API
            self.total_Requests = 0                   #Total requests proceseed by the app
            self.failed_Requests = 0
            self.statsDOIGroup = {}
            missingLinks = 0
            retDOISFull = 0                      #A DOI is retrieved if a least one link was sucessfully downloaded with full content information
            retDOISMeta = 0                      #A DOI is retrieved with metadata if no link was retrieved with full content and at least one was retrieved with metadata
            failedDOIS = 0                       #A DOI is failed if all its links were failed to be downloaded (response not in [200,300])
            dupDOIS = 0                          #A DOI is duplicated if it contains more than once in the list retrieved by crossref
            STOP = number                        #For debugging purposes only



            results = self.filter_by_issn(issn)
            #all_filenames_per_all_dois = []
            list_dois = []
            processed = 0

            for result in results:
                for item in result.message['items']:
                    doi = item['DOI']
                    filenames_per_doi = []
                    info = []
                    if doi not in list_dois:  # if doi is duplicated do not process
                        list_dois.append(item['DOI'])
                        print("\n")
                        print("DOI:" + item['DOI'])
                        processed = processed + 1
                        #if processed == 5827:
                        #    x=1
                        print ("Processed = " + str(processed) + '\n' )
                        full = 0
                        meta = 0
                        info = [str(processed), str(item['DOI'])]
                        if self.works.doi(item['DOI']).get('link') is not None:
                            for link in self.works.doi(item['DOI'])['link']:
                                print("URL:" + link['URL'])
                                print("Type:" + link['content-type'])
                                try:
                                    fn, extension = self.download_link(root_dir=root_dir,
                                                                   issn=issn,
                                                                   doi=item['DOI'],
                                                                   url=link['URL'],
                                                                   type=link['content-type'],
                                                                   time=1)
                                    filenames_per_doi.append(fn + "#" + extension + '#' + str(link['URL']))
                                    self.total_Requests = self.total_Requests + 1
                                    print("\n")
                                    if STOP>0 and processed == STOP:  # For debugging purposes:
                                        break

                                    #For debugging only -------------------------------------
                                    self.fall.write(doi + "\t" + link['URL']+ "\t" + link['content-type'] + "\t" + extension + "\n")
                                except:
                                    print("Unexpected error at DownloadbyIssn:", sys.exc_info()[0])
                                    traceback.print_exc()
                                    #--------------------------------------------------------
                            row, full, meta, fails = self.count_filetypes_by_doi(item['DOI'], filenames_per_doi)
                            info.extend(row)
                            self.failed_Requests = self.failed_Requests + fails




                        else:
                            print("No links for " + item['DOI'])
                            self.funsuc.write("\n No links for " + item['DOI'])
                            info.append('No Link provided')
                            full = -1

                        # Count info per doi.
                        # A doi is
                        # Retrieved: If at least one link was successfully saved. There can be a combination of htmls, pdfs, xmls, etc per doi
                        # but only one would be accounted for.
                        # Failed: If all get request for the doi returned error
                        # Missing: If there was not link provided from crossref
                        if full == -1:
                           missingLinks = missingLinks+1
                        else:
                            if full>0:
                                retDOISFull = retDOISFull + 1
                            else:
                                if meta>0:
                                    retDOISMeta = retDOISMeta + 1
                                else:
                                    failedDOIS = failedDOIS+1
                    else:
                        dupDOIS = dupDOIS + 1  # Counts duplicated DOIs
                        info.append('NA')
                        info.append(str(item['DOI']))
                        info.append('Duplicated DOI')
                    #info = info + '\n'

                    self.LDWriter.writerow(info)
                    #all_filenames_per_all_dois.append(info)

                    if STOP>0 and processed == STOP:  #For debugging purposes
                       break
                print("\n\n")
                if STOP>0 and processed == STOP:    #For debugging purposes
                   break


            accounted = retDOISFull + retDOISMeta + failedDOIS + missingLinks + dupDOIS  #retrievedDOIS + failedRequest + missingLinks + duplicateddois(incrossref)
            if self.total_results_DOI>0:
                proc_p = round((processed/self.total_results_DOI*100),2)
                retFull_p = round(retDOISFull/self.total_results_DOI*100,2)
                retMeta_p = round(retDOISMeta/self.total_results_DOI*100,2)
                fail_p = round(failedDOIS/self.total_results_DOI*100,2)
                nolinks_p =round(missingLinks/self.total_results_DOI*100,2)
                dup_p = round(dupDOIS/self.total_results_DOI*100,2)
                acc_p = round(accounted/self.total_results_DOI*100,2)
                unac_p = round((self.total_results_DOI - accounted)/self.total_results_DOI*100,2)
            else:
                proc_p = 0
                retFull_p = 0
                retMeta_p = 0
                fail_p = 0
                nolinks_p = 0
                dup_p = 0
                acc_p = 0
                unac_p = 0

            fail_links_p=0
            if self.total_Requests>0:
                fail_links_p=round(self.failed_Requests / self.total_Requests * 100, 2)



            self.statsDOIGroup['DOIs in Crossref'] = {self.TOTAL: self.total_results_DOI, self.TOTAL_PERC: " ", self.PDF: " ", self.HTML: " ",  self.HTML_META:" ", self.XML: " ",
                                                  self.TXT: " ",
                                                  self.UNK: " "}
            self.statsDOIGroup['DOIs Processed'] = {self.TOTAL: processed, self.TOTAL_PERC: str(proc_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                self.TXT: " ", self.UNK: " "}

            self.statsDOIGroup['DOIs Retrieved with at least one Full Content file'] = {self.TOTAL: retDOISFull,  self.TOTAL_PERC: str(retFull_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                self.TXT: " ", self.UNK: " "}


            self.statsDOIGroup['DOIs Retrieved with only Metadata'] = {self.TOTAL: retDOISMeta,  self.TOTAL_PERC: str(retMeta_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                self.TXT: " ", self.UNK: " "}


            self.statsDOIGroup['DOIs with all Links Failed'] = {self.TOTAL: failedDOIS, self.TOTAL_PERC: str(fail_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                     self.TXT: " ", self.UNK: " "}

            self.statsDOIGroup['DOIs without Links from Crossref'] = {self.TOTAL: missingLinks, self.TOTAL_PERC:  str(nolinks_p),  self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                    self.TXT: " ", self.UNK: " "}

            self.statsDOIGroup['DOIs Duplicated'] = {self.TOTAL: dupDOIS, self.TOTAL_PERC: str(dup_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                       self.TXT: " ", self.UNK: " "}

            self.statsDOIGroup['DOIs Accounted'] = {self.TOTAL: accounted, self.TOTAL_PERC: str(acc_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ", self.XML: " ",
                                                self.TXT: " ", self.UNK: " "}

            self.statsDOIGroup['DOIs Unaccounted'] = {self.TOTAL: self.total_results_DOI - accounted, self.TOTAL_PERC: str(unac_p), self.PDF: " ", self.HTML: " ", self.HTML_META:" ",
                                                  self.XML: " ",
                                                  self.TXT: " ", self.UNK: " "}




            #self.statsDOIGroup['Total Links (Requests)'] = {self.TOTAL: self.total_Requests, self.PDF: " ", self.HTML: " ",self.XML: " ", self.TXT: " ", self.UNK: " "}
            #self.statsDOIGroup['Failed Links (Requests)'] = {self.TOTAL:  self.str(fail_links_p), self.PDF: " ", self.HTML: " ", self.XML: " ", self.TXT: " ", self.UNK: " "}


            # Write all files per doi file , filename_1, filename_2,...filename_n
            #for s in all_filenames_per_all_dois:
            #    self.LDWriter.writerow(s)

            print('Total DOIs in Crossref: ' + str(self.total_results_DOI) + '\n')

        except:
            print("Unexpected error at DownloadbyIssn:", sys.exc_info()[0])
            traceback.print_exc()
        finally:
            csvfileLD.close()
            csvfile.close()
            self.fall.close()
            self.fsuc.close()
            self.funsuc.close()
            return (self.statsDOIGroup)

    def create_dir(self, dir):
        if not os.path.isdir(dir):
            os.mkdir(dir)

    def expected_extension(self, type, extension, url):

        expected = 1  # Default is true
        ext = ''

        # Obtain the expected extension
        if 'application/pdf' in type:
            ext = 'pdf'
        elif 'text/html' in type:
            ext = 'html'
        elif 'plain/text' in type:
            ext = 'txt'
        elif 'application/xml' in type or 'text/xml' in type:
            ext = 'xml'
        elif 'unspecified' in type:
            if 'pdf' in url:
                ext = 'pdf'
            elif 'xml' in url:
                ext = 'xml'
            elif 'txt' in url:
                ext = 'txt'

        if ext != '':
            if extension != ext:
                expected = 0  # False

        return (expected)


    def download_link(self, url, issn, doi, type, root_dir, time):
        try:
            fn = ''
            #extension=''
            r = requests.get(url, headers=self.hdr)
            if r.status_code >= 200 and r.status_code < 300:
                type_as_a_list = type.split('/')
                if 'application/pdf' in r.headers['Content-Type']:
                    extension = 'pdf'
                elif 'text/html' in r.headers['Content-Type']:
                    extension = 'html'
                elif 'plain/text' in r.headers['Content-Type']:
                    extension = 'txt'
                elif 'application/xml' in r.headers['Content-Type'] or 'text/xml' in r.headers['Content-Type']:
                    extension = 'xml'
                elif len(type_as_a_list) == 2:
                    extension = type_as_a_list[1]
                    if extension == 'plain':
                        extension = 'txt'
                elif 'pdf' in r.url:
                    extension = 'pdf'
                elif 'xml' in r.url:
                    extension = 'xml'
                elif 'html' in r.url:
                    extension = 'html'
                elif 'txt' in r.url:
                    extension = 'txt'
                else:
                    extension = 'unknown'

                doi_split = doi.split('/')
                extension_dir=''
                try:
                    if extension == 'xml':
                        t = ElementTree.fromstring(r.text)
                except:
                    extension_dir = "XML_TRUNC"
                    print(doi + "\t XML INVALID/TRUNCATED \n")
                    self.funsuc.write(doi + "\t" + url + "\t" + "XML INVALID/TRUNCATED" + "\n")

                marked = 0
                if not self.expected_extension(type, extension, url):
                    extension_dir = extension_dir + "HTML_METADATA"
                    row = [str(doi), str(url), str(r.status_code), "No Content"]
                    self.retryWriter.writerow(row)
                    marked = 1
                else:
                    if extension == 'html':
                        content = self.tdm_html.HtmlFullContent(r)
                        if not content:
                            extension_dir = extension_dir + "HTML_METADATA"
                            row = [str(doi), str(url), str(r.status_code), "No Content"]
                            self.retryWriter.writerow(row)
                            marked = 1

                base_dir = root_dir + "/" + issn
                self.create_dir(root_dir)
                self.create_dir(base_dir)

                if len(doi_split) == 2:
                    doi_group = doi_split[0]
                    doi_id = doi_split[1]
                    doi_path = base_dir + "/" + doi_group
                    self.create_dir(doi_path)
                    doi_path = doi_path + "/" + extension_dir + "/"
                    self.create_dir(doi_path)

                    fn = doi_path + doi_id + '.' + extension
                    with open(fn, 'wb') as output:
                        output.write(r.content)
                    self.fsuc.write(doi + "\t" + url + '\t' + extension + '\n')

                    if marked==1:
                        extension = self.HTML_META_EXT
            else:
                if time == 1 and r.status_code==401:  #401 ==  Resource Not found.
                    print("Failed Request: " + url + " for DOI " + doi + " due to error code " + str(r.status_code))
                    self.funsuc.write(doi + "\t" + url + "\t" + str(r.status_code) + "\t Failed Request" "\n")
                    row = [str(doi), str(url), str(r.status_code), 'FAILED']
                    self.retryWriter.writerow(row)
                    urlDOI = 'https://doi.org/' + doi
                    self.total_Requests = self.total_Requests+1
                    self.failed_Requests = self.failed_Requests + 1
                    fn, extension = self.download_link(urlDOI, issn, doi, type, root_dir, 2)
                else:
                    print("Failed Request: " + url + " for DOI " + doi + " due to error code " + str(r.status_code))
                    self.funsuc.write(doi + "\t" + url + "\t" + str(r.status_code) + "\t Failed Request" "\n")
                    row = [str(doi), str(url), str(r.status_code), 'FAILED']
                    self.retryWriter.writerow(row)
                    fn = 'Response Code - ' + str(r.status_code)
                    extension = self.FAIL
        except:
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_exc()
        finally:
            return fn, extension
