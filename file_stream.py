import glob
import re
import os
import fnmatch

path = "D:/SAP-C/UpStream/Database"
file_list = [os.path.join(dirpath, f)
    for dirpath, dirnames, files in os.walk(path)
    for f in fnmatch.filter(files, '*.sql')]
#new_path = path + "/NEW"
#file_list = glob.glob(path + "*.sql")
for f in file_list:
    print("reading from {}".format(f))
    fp = open(f, encoding = 'utf-8', mode='r')
    nf = re.sub('Database', 'Database_NEW', os.path.split(f)[0]) + "/" + os.path.split(f)[1]
    print("writing into {}".format(nf))
    new_fp = open(nf, encoding = 'utf-8', mode = 'w')
    string = fp.read()

    #########
    string = re.sub('MODEL', 'REFERENCE', string)
    string = re.sub('ODS', 'FINHUB', string)
    string = re.sub('SRC\.', 'FINHUB.', string)
    string = re.sub('TSDMDL16K', 'TSDCTL16K', string)
    string = re.sub('TSIMDL16K', 'TSICTL16K', string)
    #########

    new_fp.write(string)
    fp.close()
    new_fp.close()

