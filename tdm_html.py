from bs4 import BeautifulSoup
import traceback
import sys

class TdmHtml:
    def __init__(self):
        self.abstract_maximum_words = 800
        self.article_minimum_words = 5000

    def HtmlFullContent(self, htmlpage):
        full = 1   #Default is yes.
        try:
            page = htmlpage.text
            soup = BeautifulSoup(page, 'html.parser')
            body = soup.find('body')
            body_text = body.get_text()
            body_text_list = body_text.split()

            #file_name = "/Users/jazminromero/PycharmProjects/Testing/textHtml.txt"
            #f = open(file_name, "w+")
            #f.write(str(body_text))

            #file_name = "/Users/jazminromero/PycharmProjects/Testing/textHtml.txt"
            #f = open(file_name, "w+")
            #f.write(str(body_text))


            #print("\n\n")

            abstract_keywords = ['Abstract','ABSTRACT']
            index = 0
            count_abstract = 0
            abstract_indices = []
            for s in body_text_list:
                st = str(s)
                if any(aw in st for aw in abstract_keywords):
                    count_abstract = count_abstract + 1
                    #print('Abstract Occurrence ' + str(count_abstract) + ' = ' + str(index))
                    abstract_indices.append(str(index))
                index = index + 1

            #print("\n\n")

            if count_abstract==0:
                #print('The HTML does not contain the keyword Abstract')
                #print('Probably an HTML without full content')
                if len(body_text_list) > self.article_minimum_words:
                    full=1
                else:
                    full=0
                return full


            references_keywords = ['References','REFERENCES']

            if not any(rw in body_text for rw in references_keywords):
                references_keywords = ['Citations','CITATIONS']
                if not any(rw in body_text for rw in references_keywords):
                    references_keywords = ['Citation','CITATION']
                    if not any(rw in body_text for rw in references_keywords):
                        references_keywords = ['Bibliography', 'BIBLIOGRAPHY']
                        if not any(rw in body_text for rw in references_keywords):
                            #print('The HTML does not contain the keyword references/citations/citation')
                            #print('Probably an HTML without full content')
                            full=0
                            return full

            index = 0
            count_references = 0
            references_indices = []
            for s in body_text_list:
                st = str(s)
                if any(rw in st for rw in references_keywords):
                    count_references = count_references + 1
                    #print('References Occurrence ' + str(count_references) + ' = ' + str(index))
                    references_indices.append(str(index))
                index = index + 1

            last_reference = int(references_indices[count_references - 1])
            last_abstract = int(abstract_indices[0])
            for a in abstract_indices:
                last_abstract_temp = int(a)
                if last_abstract_temp < last_reference:
                    last_abstract = last_abstract_temp



            if count_references >= 2 and int(references_indices[count_references - 2]) > last_abstract:
                last_reference = int(references_indices[count_references - 2])

            #print('\nIndex of considered last abstract occurrence: ' + str(last_abstract))
            #print('Index of last reference occurrence: ' + str(last_reference))

            words = last_reference - last_abstract - 1
            #print('\n\nEstimated number of words between Abstract and References: ' + str(words))

            if words <= self.abstract_maximum_words:
                #print('HTML without full article content')
                full = 0
            else:
                #print('Full article content in HTML')
                full = 1
        except:
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_exc()

        return full
