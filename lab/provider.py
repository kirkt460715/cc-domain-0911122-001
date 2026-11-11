import hashlib,threading
from .http import ProtocolError,main
from registry_bridge.feeds.message import digest,parse_message
class Provider:
    def __init__(self):
        self.lock=threading.RLock();self.revision=1;self.oldest=1;self.epoch='exchange-1';self.calls=[];self.messages=[]
        for i in range(1,33):
            self.messages.append({'version':1,'event_id':f'north:event{i}','issuer':'north','sequence':i,'asset_id':f'asset{i}','revision':1,'operation':'register','effective_at':'2026-11-01T00:00:00Z','payload':{'title':f'合成数据集{i}','holder':'holder1','dataset_fingerprint':hashlib.sha256(str(i).encode()).hexdigest(),'scope':{'purposes':['research'],'regions':['CN'],'rights':['hold','process'],'expires_at':'2027-11-01T00:00:00Z'},'evidence':[{'kind':'agreement','sha256':'b'*64}]}})
    def handle(self,method,path,args,body):
        with self.lock:
            self.calls.append({'method':method,'path':path,'args':args})
            if path=='/control' and method=='POST':
                if body.get('expire'):self.revision+=1;self.oldest=self.revision
                return {'revision':self.revision}
            if path=='/view':return {'epoch':self.epoch,'revision':self.revision,'ceiling':32,'issuer':'north'}
            if args.get('epoch')!=self.epoch or not self.oldest<=int(args.get('revision',0))<=self.revision:raise ProtocolError(410,'view_expired')
            lo=int(args['lower']);hi=int(args['upper'])
            if not 1<=lo<=hi<=32:raise ProtocolError(400,'range')
            rows=self.messages[lo-1:hi]
            if path=='/summary':return {'lower':lo,'upper':hi,'sha256':digest([[x['sequence'],parse_message(x).fingerprint] for x in rows]),'count':len(rows),'revision':int(args['revision'])}
            if path=='/bundle':
                value={'format':'registry-event-bundle','version':1,'issuer':'north','display_name':'合成北方','lower':lo,'upper':hi,'messages':rows}
                return {'body':value,'sha256':digest(value)}
            raise ProtocolError(404,'route')
if __name__=='__main__':main(Provider())
