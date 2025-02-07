#!/usr/bin/python3
import shlex,os,argparse,datetime,subprocess
from subprocess import Popen, PIPE,call,check_output
from pathlib import Path
path = Path(__file__).parent.absolute()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Build Docker file for variant filtering")

    parser.add_argument("--image", type= str,
                        help="name of image",default = 'kanta')
    parser.add_argument("--version", type= str,
                        help="version value, e.g.0.001",required = True)
    parser.add_argument("--push",action = 'store_true')
    parser.add_argument("--args",type = str,default = '')
    args = parser.parse_args()

    
    basic_cmd = 'docker build -t eu.gcr.io/finngen-refinery-dev/' + args.image +':' +args.version
    cmd = basic_cmd + f" -f {os.path.join(path,'Dockerfile')} {path.parent} {args.args} "
    print(cmd)
    call(shlex.split(cmd))

    if args.push:
        cmd = ' docker -- push eu.gcr.io/finngen-refinery-dev/' + args.image +':' + args.version
        print(cmd)
        call(shlex.split(cmd))
