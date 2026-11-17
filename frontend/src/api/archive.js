import{api}from'./client.js';
export const listBackups=()=>api('/archives/backups');
export const createBackup=kind=>api('/archives/backups',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({kind})});
export const listRestores=()=>api('/archives/restores');
export function backupUrl(packageId){const session=JSON.parse(localStorage.getItem('lsm-session')||'null');return fetch('/api/archives/backups/'+packageId+'/download',{headers:session?{Authorization:'Bearer '+session.token}:{}})}
export async function uploadRestore(file){
  const session=JSON.parse(localStorage.getItem('lsm-session')||'null');
  const form=new FormData();form.append('file',file);
  const response=await fetch('/api/archives/restores',{method:'POST',body:form,headers:session?{Authorization:'Bearer '+session.token}:{}});
  if(!response.ok){const data=await response.json().catch(()=>({detail:'服务暂时不可用，请重试'}));throw new Error(typeof data.detail==='string'?data.detail:JSON.stringify(data.detail))}
  return response.json()
}
export const applyRestore=packageId=>api('/archives/restores/'+packageId+'/apply',{method:'POST'});
