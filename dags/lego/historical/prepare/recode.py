import sys
def transcode( in_file, out_file):
    fo = open(out_file, 'w', encoding='utf-8')
    with open(in_file, 'rb')  as fr:
        for line in fr:
            text1 = line.decode('GB2312', errors="ignore").encode('utf-8', errors='ignore').decode('utf-8').replace('\r','')
            fo.write(text1)   

    fo.flush()
    fo.close() 

if __name__ == "__main__":
    args = sys.argv
    print(args)
    if (len(args) != 3):
        print('args wrong')
        exit()

    in_file = args[1]
    out_file = args[2]   
    transcode(in_file, out_file)