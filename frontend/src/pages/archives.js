import{archiveStore}from'../stores/archive.js';import{createBackup,backupUrl,uploadRestore,applyRestore}from'../api/archive.js';import{header}from'../components/common/header.js';import{table}from'../components/common/table.js';import{esc}from'../utils/html.js';import{notify,submit}from'../hooks/feedback.js';import{session}from'../stores/session.js';
const JOB_STATUS={pending:'待校验',applying:'恢复中',available:'可用',rejected:'整包拒绝'};
const KIND={full:'全量',incremental:'增量'};
function kindBadge(k){return `<span class="badge ${k==='full'?'passed':''}">${KIND[k]||k}</span>`}
function jobBadge(s){return `<span class="badge ${s==='rejected'?'failed':s==='available'?'passed':''}">${JOB_STATUS[s]||s}</span>`}
function blob(packageId,name){backupUrl(packageId).then(async r=>{if(!r.ok)throw new Error((await r.json().catch(()=>({detail:'下载失败'}))).detail);const url=URL.createObjectURL(await r.blob());const a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}).catch(e=>notify(e.message))}
export async function archivesPage(el){
  const isAdmin=session.get().role==='admin';
  await archiveStore.load();const {backups,restores}=archiveStore;
  el.innerHTML=header('05','换机离线带走与恢复试验档案','全量包包含全部已完成试验及其引用批次与配置；增量包只含上一包之后新增对象，依赖前置包逐包恢复。每个包都有内容签名，缺祖先、被篡改或引用缺失时整包拒绝。')
  +`<div class="split">
  <section class="panel"><h2>备份包（可下载离线转移）</h2>
    <div class="actions" style="margin:0 0 18px">
      <button class="primary" id="full" ${isAdmin?'':'disabled'}>制作全量备份</button>
      <button id="incr" ${isAdmin?'':'disabled'}>制作增量备份</button>
      <button id="refresh">刷新</button>
    </div>
    <p class="helper">备份采用单一时点快照：制作期间仍可创建试验，但快照之后新增的对象留给下一包。已完成（有结果）的试验才入包。</p>
    ${backups.length?table(['#','类型','对象数（批次/配置/试验）','前置包','时间','下载'],backups.map(x=>[x.sequence,kindBadge(x.kind),`${x.counts.datasets} / ${x.counts.configs} / ${x.counts.trials}`,x.parent_id?esc(x.parent_id.slice(0,8))+'…':'—',esc(x.created_at),`<a href="javascript:void(0)" data-dl="${x.package_id}" data-name="${esc(x.file_name)}">下载 ↓</a>`])):'<div class="empty">还没有备份包。管理员可先制作全量备份。</div>'}
  </section>
  <aside class="panel"><h2>恢复到本工坊</h2>
    <form id="restore-form" ${isAdmin?'':'aria-disabled'}>
      <label>选择归档包 JSON（全量或增量）</label>
      <input type="file" name="file" accept="application/json,.json" ${isAdmin?'':'disabled'}>
      <div class="actions"><button class="primary" type="submit" ${isAdmin?'':'disabled'}>上传待校验 →</button></div>
    </form>
    <p class="helper">上传后进入“待校验”，确认祖先链与引用完整后才写入。同名同内容直接复用；同名异内容自动另存，原有试验不受影响。</p>
    ${restores.length?table(['包序号','类型','状态','结果','操作'],restores.map(x=>[x.sequence||'—',kindBadge(x.kind||'full'),jobBadge(x.status),x.detail?esc(x.detail):'—',x.status==='pending'?(isAdmin?`<a href="javascript:void(0)" data-apply="${x.package_id}">校验并恢复 →</a>`:'—'):(x.status==='rejected'&&isAdmin?'补齐条件后重新上传':'—')])):'<div class="empty">暂无恢复任务。</div>'}
    <p class="helper" id="job-note"></p>
  </aside></div>`;
  el.querySelector('#refresh').onclick=()=>archivesPage(el);
  el.querySelectorAll('[data-dl]').forEach(a=>a.onclick=()=>blob(a.dataset.dl,a.dataset.name));
  el.querySelectorAll('[data-apply]').forEach(a=>a.onclick=()=>submit(a,async()=>{try{const r=await applyRestore(a.dataset.apply);notify(`恢复完成：新增试验 ${r.restored.trials}，复用 ${r.reused}，另存 ${r.renamed}`)}finally{await archivesPage(el)}}));
  if(isAdmin){
    el.querySelector('#full').onclick=()=>submit(el.querySelector('#full'),async()=>{await createBackup('full');notify('全量备份已生成');await archivesPage(el)});
    el.querySelector('#incr').onclick=()=>submit(el.querySelector('#incr'),async()=>{try{await createBackup('incremental');notify('增量备份已生成')}finally{await archivesPage(el)}});
    el.querySelector('#restore-form').onsubmit=e=>{e.preventDefault();const f=e.target.file.files[0];if(!f)return notify('请先选择归档包');submit(e.target,async()=>{try{await uploadRestore(f);notify('已上传，进入待校验')}finally{await archivesPage(el)}})};
  }
}
