import contextlib,json,threading
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from urllib.parse import urlsplit,parse_qs
class ProtocolError(Exception):
    def __init__(self,status,code):self.status=status;self.code=code
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):self.respond()
    def do_POST(self):self.respond()
    def respond(self):
        p=urlsplit(self.path);args={k:v[-1] for k,v in parse_qs(p.query).items()}
        try:
            size=int(self.headers.get('Content-Length','0'))
            if size>1048576:raise ProtocolError(413,'body_limit')
            body=json.loads(self.rfile.read(size)) if size else {}
            result=self.server.provider.handle(self.command,p.path,args,body)
            if result is None:self.connection.shutdown(2);self.connection.close();return
            data=json.dumps(result,ensure_ascii=False).encode();status=200
        except ProtocolError as e:status=e.status;data=json.dumps({'error':e.code}).encode()
        except (ValueError,KeyError,TypeError):status=400;data=b'{"error":"invalid_request"}'
        self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
@contextlib.contextmanager
def serving(provider):
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.provider=provider
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield 'http://127.0.0.1:'+str(server.server_port)
    finally:server.shutdown();server.server_close();thread.join()
def main(provider):
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=8765);a=p.parse_args()
    server=ThreadingHTTPServer(('127.0.0.1',a.port),Handler);server.provider=provider
    print('本地服务端口',server.server_port,flush=True);server.serve_forever()
