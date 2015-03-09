path = 'D:\Privacy_Test\Initial_files'
files = ['20140925_EBI_Claim_ff_ds.out.txt',
         '20140925_EBI_Client_ff_ds.out.txt',
         '20140925_EBI_Policy_ff_ds.out.txt',
         '20140925_EBI_Risk_ff_ds.out.txt',
         '20140925_MDL_Claim_ff_ds.out.txt',
         '20140925_MDL_Client_ff_ds.out.txt',
         '20140925_MDL_Policy_ff_ds.out.txt',
         '20140925_MDL_Risk_ff_ds.out.txt']
pos = [4,5,6,4,4,5,6,4]


prod = ['CCI','CTP','CAP','CRO','MON','CON','MBD','SME','GLB','MOT','HPK']

w3 = open('log.txt', 'w')

for prd in prod:
    i = 0
    for s in files:
            f = open(s,'r')
            w = open(prd+'_'+s, 'w')
            ##w2 = open(prd+'_debug_'+s, 'w')
            w3.write(str(pos[i])+"_"+str(i)+"\n")
            for line in f:
                ##w2.write(line.split('|')[pos[i]]+'\n')
                
                if prd in line.split('|')[pos[i]]:
                    w.write(line)
            i = i+1
            w.flush()
            ##w2.flush()
            ##w2.close()
            
            w.close()
            f.close()
w3.flush()
w3.close()
