// Prepared for the existing 03 logout path; never alters room or grant data.
export function clearSolo04Access({eventTarget=globalThis.window,storage,channelFactory=name=>new BroadcastChannel(name)}={}){
  try{(storage||globalThis.localStorage)?.removeItem('receiver04-capability-v1');}catch{}
  eventTarget?.dispatchEvent(new Event('receiver03:logout'));
  try{const channel=channelFactory('receiver04-access-v1');channel.postMessage({type:'logout'});channel.close();}catch{}
}
