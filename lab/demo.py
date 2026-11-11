import json,urllib.request
from .provider import Provider
from .http import serving
def request(base,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=3) as response:return json.load(response)
def main():
    with serving(Provider()) as base:
        print('本地来源:',base)
        print(json.dumps(request(base,'/view',None),ensure_ascii=False))
if __name__=='__main__':main()
