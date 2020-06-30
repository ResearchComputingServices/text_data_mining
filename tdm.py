#
# Entry point to the application
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#
# Example of creating a virtual environment on Windows:
#   mkvirtualenv -p C:\Users\sergiubuhatel\AppData\Local\Programs\Python\Python37\python.exe tdm
#
# Example of running this code
#   python tdm.py -r c:\development\results_tdm -i c:\development\tdm\issn_file.csv -d 1 -s 1
#

import sys, getopt
import pandas as pd
from tdm_works import TdmWorks
from tdm_retry import TdmRetry

def help():
   print('python tdm.py -r <root_dir> -i <issn_file> -d <retr,number> -s <scr>')

def main(argv):
   root_dir = ''
   issn_file = ''
   statsISSN = {}
   infoISSN = {}
   pd.options.display.width = 0

   try:
      opts, args = getopt.getopt(argv,"r:i:d:s:")
   except getopt.GetoptError:
      help()
      sys.exit(2)
   for opt, arg in opts:
      if opt in ("-r", "--root"):
         root_dir = arg
      else:
         if opt in ("-i", "--issn_file"):
            issn_file = arg
         else:
            if opt in ("-d", "--retr"):
               retrieving = arg
            else:
               if opt in ("-s", "--scr"):
                  retrying = arg

   print('\n\n<-------Retrieving Step ----------------------------------------->\n')

   print('Root:', root_dir)
   print('Issn_file:', issn_file)
   print('Retrieving:', retrieving)
   print('Scrapping:', retrying)
   print("\n")

   number = 0
   if ',' in retrieving:
      retrieving_list = retrieving.split(',')
      retrieving= retrieving_list[0]
      number = int(retrieving_list[1])


   dfISSN = pd.read_csv(issn_file)


   #Retrieving Step
   if retrieving=='1':
      tdm_works = TdmWorks()
      file_name = root_dir + "/Stats.xls"

      journal_column = dfISSN.columns.get_loc('Journal_Name')
      publisher_column = dfISSN.columns.get_loc('Publisher_ABDC')
      irow = 0
      for issn in dfISSN['ISSN']:
       journal_name = dfISSN.iloc[irow, journal_column]
       publisher_name = dfISSN.iloc[irow, publisher_column]
       print('ISSN:  ' + str(issn) + ' Journal: ' + journal_name + ' Publisher: ' + publisher_name + '\n')

       sI = tdm_works.download_by_issn(root_dir=root_dir, issn=issn, number=number)

       statsISSN[issn] = sI
       infoISSN[issn]={0: " ", 1:" "}
       infoISSN[issn][0]= journal_name
       infoISSN[issn][1]= publisher_name
       irow = irow + 1
       print('-------------------------------------------------------------------------')

      if len(statsISSN) > 0:
         list_issn =[]
         frames=[]

         for i,value in statsISSN.items():     #key,value
            list_issn.append(i)
            df = pd.DataFrame.from_dict(value,orient='index')
            frames.append(df)

         df = pd.concat(frames, keys=list_issn, names=['ISSN'])
         dfI = pd.DataFrame.from_dict(infoISSN, orient='index')

         df.rename(columns={0: 'TOTAL', 1: '%' , 2: 'PDF', 3: "HTML", 4: "HTML (METADATA)", 5: "XML", 6:"TXT", 7:"UNKNOWN"}, inplace=True)
         dfI.rename(columns={0: 'JOURNAL NAME', 1: 'PUBLISHER'}, inplace=True)

         print("\n")
         print(df)

         dfF = df.join(dfI, on=['ISSN'])

         if not df.empty:
            dfF.to_excel(file_name)


   if retrying=='1':

      print('\n\nRetrying Step -----------------------------------------\n')
      #Retry Step
      tdm_retry = TdmRetry()
      statsISSN_Retry = {}

      file_name = root_dir + "/Retry_Stats.xls"

      for issn in dfISSN['ISSN']:
         print ('ISSN:  ' + str(issn) + '\n')
         sI = tdm_retry.retry_to_download(root_dir=root_dir, issn=issn)
         if len(sI)>0:
            statsISSN_Retry[issn] = sI
         print ('-------------------------------------------------------------------------')

      if len(statsISSN_Retry)>0:
         list_issn = []
         frames = []

         for i, value in statsISSN_Retry.items():  # key,value
            list_issn.append(i)
            df = pd.DataFrame.from_dict(value, orient='index')
            frames.append(df)

         df = pd.concat(frames, keys=list_issn)
         df.rename(columns={0: 'PDF', 1: "HTML", 2: "XML", 3: "TXT", 4: "UNKNOWN"}, inplace=True)

         print("\n")
         print(df)

         if not df.empty:
            df.to_excel(file_name)




if __name__ == "__main__":
   if len(sys.argv) != 9:
      help()
   else:
      main(sys.argv[1:])

