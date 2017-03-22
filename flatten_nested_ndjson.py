def save_flatten_json(y, fname):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(json.loads(y))
    #return out
    with open(fname, "a") as myfile:
        #print(json.dumps(out)+ '\n')
        myfile.write(json.dumps(out)+ '\n')

def flatten_file(fname):
    import os
    import json
    fname_parts = os.path.splitext(os.path.basename(fname))
    fname_joined = str.join('', (os.path.dirname(fname), os.sep, fname_parts[0], "_flat", fname_parts[1]))
    with open(fname) as f:
        for line in f:
            save_flatten_json(line, fname_joined)
         
def flatten_file_parallel(fname):
    import os
    import json
    from multiprocessing import Pool, Manager
    pool = Pool()
    mgr = Manager()
    ns = mgr.Namespace()

    fname_parts = os.path.splitext(os.path.basename(fname))
    fname_joined = str.join('', (os.path.dirname(fname), os.sep, fname_parts[0], "_flat", fname_parts[1]))
    with open(fname) as f:
        for line in f:
            pool.apply_async(save_flatten_json, [line, fname_joined])

    pool.close()
    pool.join()
    pool.terminate()


flatten_file("xyz.json")  
flatten_file_parallel("xyz.json")
