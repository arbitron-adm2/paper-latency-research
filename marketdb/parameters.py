"""Public Paper parameter snapshots. No account, authorization or write endpoint."""
import asyncio, hashlib, json, time
import httpx

BASE='https://exchange.papertrade.xyz'
async def poll(save,event,stop):
    headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'}
    async with httpx.AsyncClient(headers=headers,timeout=12,follow_redirects=False) as client:
        next_deployment=0
        while not stop.is_set():
            started=time.time()*1000
            try:
                response=await client.get(BASE+'/state/trading');response.raise_for_status()
                body=response.json()
                if body.get('schemaVersion')!=1 or not isinstance(body.get('instruments'),list):
                    raise ValueError('Unsupported trading parameter schema')
                save('paper_parameters',{'captureType':'http_snapshot','url':str(response.url),'request_started_ms':started,
                     'response_completed_ms':time.time()*1000,'body':body,'raw_body':response.text})
                if time.monotonic()>=next_deployment:
                    import re
                    root=await client.get(BASE+'/',headers={'Accept':'text/html'});root.raise_for_status()
                    paths=list(dict.fromkeys(re.findall(r'/assets/(?:build|deployment)-[A-Za-z0-9_-]+\.js',root.text)))
                    bodies={}
                    for asset in paths:
                        r=await client.get(BASE+asset);r.raise_for_status();bodies[asset]={'body':r.text,'sha256':hashlib.sha256(r.content).hexdigest()}
                    save('paper_metadata',{'captureType':'public_deployment','url':BASE+'/',
                         'html_sha256':hashlib.sha256(root.content).hexdigest(),'assets':bodies,'asset_references':paths})
                    next_deployment=time.monotonic()+3600
            except Exception as exc:
                event('parameter_poll_error',type=type(exc).__name__,detail=str(exc)[:180])
            try:await asyncio.wait_for(stop.wait(),10)
            except asyncio.TimeoutError:pass
