import pandas as pd
import requests
import os
import sys
import traceback
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse
from tdm_base import TdmBase
from tdm_html import TdmHtml
import csv
from os import path

class TdmRetry(TdmBase):
    def __init__(self):
        self.PDF = 0
        self.HTML = 1
        self.XML = 2
        self.TXT = 3
        self.UNK = 4
        self.FAIL = "FAIL"
        self.statsDOIGroup = {}  # Dictionary that keeps the count of file per type
        self.tdm_html = TdmHtml()
        super().__init__()

    def create_dir(self, dir):
        if not os.path.isdir(dir):
            os.mkdir(dir)

        # Given a list of filenames per doi which can include repeated files
        # Returns how many files per doi are, one file per type only.
    def count_filetypes_by_doi(self, doiFilenameList):
            dois_retrieved = set()
            doiFileNameSet = set(doiFilenameList)
            not_fails = 0
            failed = 0
            info = ""
            for fn in doiFileNameSet:
                if "#" in fn:
                    fn_as_list = fn.split('#')
                    if len(fn_as_list) == 3:
                        doi = fn_as_list[0]

                        if doi not in dois_retrieved:
                            dois_retrieved.add(doi)

                        extension = fn_as_list[2]
                        if extension != self.FAIL:
                            if extension == "pdf":
                                t = self.PDF
                            elif extension == "html":
                                t = self.HTML
                            elif extension == "xml":
                                t = self.XML
                            elif extension == "txt":
                                t = self.TXT
                            else:
                                t = self.UNK
                            doi_split = doi.split('/')
                            doigroup = doi_split[0]
                            if not doigroup in self.statsDOIGroup:
                                self.statsDOIGroup[doigroup] = {self.PDF: 0, self.HTML: 0, self.XML: 0, self.TXT: 0,
                                                                self.UNK: 0}
                            self.statsDOIGroup[doigroup][t] = self.statsDOIGroup[doigroup][t] + 1
                            not_fails = not_fails + 1
                            fn = fn_as_list[1]
                        else:
                            failed = failed + 1
                            # doiFilenameList.append(fn + "\t")
                            info = info + fn + "\t" + extension + "\t"  # To be recorded by file_nameLDois
            return (dois_retrieved, not_fails, failed)


    def obtain_url_doi(self, st):
        st_list = st.split()
        url =""
        doi=""
        if len(st_list)==2:
            url = st_list[0]
            doi = st_list[1]
        return url, doi


    def retry_to_download(self, root_dir, issn):

        try:
            # Read retry file
            file_nameRetry = root_dir + "/" + issn + "_FaultyURLS.csv"

            if path.exists(file_nameRetry):

                try:
                    df = pd.read_csv(file_nameRetry)
                except:
                    print ('Corrupted File (check formatting): ' + file_nameRetry)
                    print ('Bad lines will be ignored \n')
                    df = pd.read_csv(file_nameRetry, error_bad_lines=False)

                self.statsDOIGroup = {}
                file_nameRetry2 = root_dir + "/" + issn + "_ScrappingFailed.csv"
                file_nameSuccess = root_dir + "/" + issn + "_ScrappingSuccess.csv"


                csvfile = open(file_nameRetry2, 'w', newline='')
                self.retryWriter = csv.writer(csvfile, delimiter=',')
                self.retryWriter.writerow(['DOI', 'URL', 'RESPONSE CODE', 'REASON'])

                csvfileSuc = open(file_nameSuccess, 'w', newline='')
                self.retrySucWriter = csv.writer(csvfileSuc, delimiter=',')
                self.retrySucWriter.writerow(['DOI', 'URL', 'FILE RETRIEVED'])



                total_rows = len(df.index)

                URL_COLUMN = df.columns.get_loc('URL')
                RESPONSE_COLUMN = df.columns.get_loc('RESPONSE CODE')
                DOI_COLUMN = df.columns.get_loc('DOI')

                dois_retrieved = set()
                dois_list = set()
                filenames_per_doi = []
                for row in range(0,total_rows):



                    url = df.iloc[row,URL_COLUMN]
                    response_code = df.iloc[row,RESPONSE_COLUMN]
                    doi = df.iloc[row,DOI_COLUMN]


                    print('\n')
                    print('DOI: ' + str(doi))


                    if doi not in dois_list:
                        dois_list.add(doi)

                    if response_code < 200 or response_code > 300:
                        url = 'https://doi.org/' + doi

                    r = requests.get(url, headers=self.hdr)


                    if r.status_code >= 200 and r.status_code < 300:

                        if 'application/pdf' in r.headers['Content-Type']:
                            extension = 'pdf'
                        elif 'text/html' in r.headers['Content-Type']:
                            extension = 'html'
                        elif 'plain/text' in r.headers['Content-Type']:
                            extension = 'txt'
                        elif 'application/xml' in r.headers['Content-Type'] or 'text/xml' in r.headers['Content-Type']:
                            extension = 'xml'



                        base_dir = root_dir + "/" + issn
                        self.create_dir(root_dir)
                        self.create_dir(base_dir)
                        doi_split = doi.split('/')
                        extension_dir = 'RETRIED'

                        if len(doi_split) == 2:
                            doi_group = doi_split[0]
                            doi_id = doi_split[1]
                            doi_path = base_dir + "/" + doi_group
                            self.create_dir(doi_path)
                            doi_path = doi_path + "/" + extension_dir + "/"
                            self.create_dir(doi_path)

                            extension_dir = ''
                            if extension=='html':
                                content = self.tdm_html.HtmlFullContent(r)
                                if not content:
                                    extension_dir = 'HTML_METADATA'
                                    self.create_dir(doi_path + extension_dir + "/")

                            fn = doi_path + extension_dir + "/" + doi_id + '.' + extension
                            with open(fn, 'wb') as output:
                                output.write(r.content)

                            print("File \t" + fn + "\t Retrieved \n")

                            info = [str(doi), str(url), extension]
                            self.retrySucWriter.writerow(info)

                            filenames_per_doi.append(doi + "#" + fn + "#" + extension)

                            # GET THE LINKS TO A PDF DOCUMENT FROM AN HMTL ------------------
                            list_files = []

                            if extension == "html":
                                #print("Attempting to retrieve PDF from HTML \n")
                                list_files = self.download_pdfs_from_links(issn,root_dir,url=url, doi_path=doi_path, doi_group=doi_group, doi_id=doi_id, doi = doi)

                            filenames_per_doi.extend(list_files)
                        else:
                            print("Request Failed: " + url + " for DOI " + doi + " due to error code " + str(r.status_code))
                            info = [str(doi), str(url), str(r.status_code), 'Failed request']
                            self.retryWriter.writerow(info)
                    else:
                        print("Request Failed: " + url + " for DOI " + doi + " due to error code \n" + str(r.status_code))
                        info = [str(doi), str(url), str(r.status_code), 'Failed request']
                        self.retryWriter.writerow(info)

                drp = 0
                dnrp=0
                dois_retrieved, count, fails  = self.count_filetypes_by_doi(filenames_per_doi)





                if len(dois_list)>0:
                    drp = round((len(dois_retrieved)/len(dois_list)*100),2)
                    dnrp = round(((len(dois_list)-len(dois_retrieved))/len(dois_list)*100),2)

                self.statsDOIGroup['Total DOIs'] = {self.PDF: len(dois_list), self.HTML: " ", self.XML: " ", self.TXT: " ", self.UNK: " "}
                self.statsDOIGroup['DOIs Retrieved'] = {self.PDF: len(dois_retrieved), self.HTML: str(drp), self.XML: " ",self.TXT: " ", self.UNK: " "}
                self.statsDOIGroup['DOIs No Retrieved'] = {self.PDF: len(dois_list)-len(dois_retrieved), self.HTML: str(dnrp), self.XML: " ",
                                                        self.TXT: " ", self.UNK: " "}

                csvfile.close()
                csvfileSuc.close()
            else:
                print ('File Not found: ' + str(file_nameRetry))
        except:
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_exc()
        finally:
            return self.statsDOIGroup



    def is_valid(self,url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    def get_all_links_with_pdf(self, url):
        # Returns all URLs that is found on `url` and includes somewhere in the url the word "pdf"
        # all URLs with pdf of `url`
        urls = set()
        r = requests.get(url, headers=self.hdr)

        if r.status_code >= 200 and r.status_code < 300:

            html_doc = r.content.decode("utf-8")
            soup = BeautifulSoup(html_doc, "html.parser")

            urlU = r.url  # We need to update the url, in case the response is coming from a redirected webpage
            # For example: wwww.doi.org/doi will redirect to the respective journal that contains the DOI

            # domain name of the URL without the protocol
            domain_name = urlparse(urlU).netloc

            for link in soup.find_all("a"):
                href = (link.get('href'))
                if 'pdf' in str(href):
                    if href != "" or href is not None: #Tag is not null
                        # href is not an empty tag
                         href = urljoin(urlU, href)
                         #parsed_href = urlparse(href)
                         #remove URL GET parameters, URL fragments, etc.
                         #href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
                         if self.is_valid(href):
                             if domain_name in href and href not in urls:
                                urls.add(href)
            #print(urls)
        return urls


    def download_pdfs_from_links(self, issn, root_dir, url, doi_path, doi_group,doi_id,doi):
        # Get all the links with pdf word embedded
        list = []
        print ("Getting all links to PDFs")
        links = self.get_all_links_with_pdf(url)

        if len(links)==0:
            row = [str(doi_id), str(url), ' ', 'No PDF links were found']
            self.retryWriter.writerow(row)
            print ("No PDF links were found")
        else:
            for l in links:
                r = requests.get(l, headers=self.hdr)
                if r.status_code >= 200 and r.status_code < 300:
                    # We will try to retrieve any type ---???
                    if 'application/pdf' in r.headers['Content-Type']:
                        extension = 'pdf'
                    elif 'text/html' in r.headers['Content-Type']:
                        extension = 'html'
                    elif 'plain/text' in r.headers['Content-Type']:
                        extension = 'txt'
                    elif 'application/xml' in r.headers['Content-Type'] or 'text/xml' in r.headers['Content-Type']:
                        extension = 'xml'
                    else:
                        extension = 'unknown'

                    #Save only PDFs
                    if extension!='pdf':
                        print('Request not returned a PDF File for link : ' + l + '\n')
                        row = [str(doi_id), str(l), str(r.status_code), 'File retrieved was not pdf']
                        self.retryWriter.writerow(row)
                    else:
                        base_dir = root_dir + "/" + issn
                        self.create_dir(root_dir)
                        self.create_dir(base_dir)

                        self.create_dir(doi_path)
                        fn = doi_path + doi_id + '.' + extension
                        with open(fn, 'wb') as output:
                            output.write(r.content)
                            x= urlparse(l)
                            print("Link: " + l + ' returned  ' + fn + "\n")
                            list.append(doi + '#' + fn + '#' + extension)
                            info = [str(doi_group)+'/'+str(doi_id), str(l), extension]
                            self.retrySucWriter.writerow(info)
                else:
                    print("Request Failed: " + l + " for DOI " + doi_id + " due to error code " + str(r.status_code))
                    row = [str(doi_id), str(l), str(r.status_code), 'Failed request']
                    self.retryWriter.writerow(row)


        return(list)


