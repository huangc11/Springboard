
class  logParser:
    def __init__(self, filepath):
        self.filepath= filepath


    def parse(self):

        f = open(self.filepath)
        lines = f.readlines()
        errors=[]
        num_error=0
        for line in lines:
            words = line.split()
            try:
                # print(words[3] )
                if words[3] == 'ERROR':
                    l = len(words)
                    s = ''
                    for i in range(3,l):
                        s +=' ' + words[i]
                    errors.append(s)
                    num_error+=1

            except:
                pass

        if len(errors)>0:
            return (num_error, errors)
        else:
            return (0, [])



if __name__=='__main__':

    fp="c:/ubtemp/airflowlog/erro1.log"
    p = logParser(fp)

    (count, errors) = p.parse()

    print('Total number of errors: {}'.format(count))
    print ('log name: {}'.format(fp))
    for e in errors:
        print (e)
