"""离线归档包的封包与验包纯函数。

包是单个 JSON 文档：manifest（清单与对象指纹索引）+ payload（完整业务内容）+ signature。
signature 覆盖规范化后的 manifest 与 payload，任何字节级篡改都会在验包时暴露。
内容指纹只取业务内容（不含本地 id 与 created_at），跨工坊仍可据此判重。
"""
import hashlib,hmac,json,uuid
from datetime import datetime
FORMAT='lsm-archive/v1'
KINDS=('full','incremental')
TYPES=('dataset','config','trial')
class ArchiveError(Exception):
    """归档包不合法或校验失败；路由层统一映射为整包拒绝。"""
def canonical(obj)->bytes:
    return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
def digest(data:bytes)->str:return hashlib.sha256(data).hexdigest()
def content_hash(otype:str,obj:dict)->str:
    view={k:v for k,v in obj.items() if k!='created_at'}
    return digest(canonical({'type':otype,'content':view}))
def _index(otype,rows):return [{'type':otype,'name':x['name'],'content_hash':content_hash(otype,x)} for x in rows]
def build_package(kind,sequence,parent_id,datasets,configs,trials,watermarks,package_id=None):
    if kind not in KINDS:raise ArchiveError('未知备份类型')
    if kind=='incremental' and not parent_id:raise ArchiveError('增量包必须携带祖先包标识')
    manifest={'format':FORMAT,'package_id':package_id or uuid.uuid4().hex,'kind':kind,
              'sequence':sequence,'parent_id':parent_id,'created_at':datetime.now().isoformat(timespec='seconds'),
              'workshop':'lsm-workshop','watermarks':watermarks,
              'counts':{'datasets':len(datasets),'configs':len(configs),'trials':len(trials)},
              'objects':_index('dataset',datasets)+_index('config',configs)+_index('trial',trials)}
    payload={'datasets':datasets,'configs':configs,'trials':trials}
    signature=digest(canonical(manifest)+b'.'+canonical(payload))
    return {'format':FORMAT,'manifest':manifest,'payload':payload,'signature':signature}
def encode(doc)->bytes:
    return json.dumps(doc,ensure_ascii=False,indent=2).encode('utf-8')
def decode(raw:bytes)->dict:
    try:return json.loads(raw.decode('utf-8'))
    except Exception as exc:raise ArchiveError('归档包不是合法 JSON，整包拒绝') from exc
def verify(raw:bytes)->dict:
    """完整验包：结构、签名、对象指纹索引与载荷逐一核对，任何一项不符即抛 ArchiveError。"""
    doc=decode(raw)
    if not isinstance(doc,dict) or set(doc)!={'format','manifest','payload','signature'}:raise ArchiveError('归档包结构不完整，整包拒绝')
    if doc.get('format')!=FORMAT:raise ArchiveError('归档格式或版本不受支持，整包拒绝')
    manifest,payload=doc.get('manifest'),doc.get('payload')
    if not isinstance(manifest,dict) or not isinstance(payload,dict):raise ArchiveError('清单或载荷缺失，整包拒绝')
    for key in ('package_id','kind','sequence','parent_id','objects','counts','watermarks'):
        if key not in manifest:raise ArchiveError(f'清单缺少 {key}，整包拒绝')
    if manifest['kind'] not in KINDS:raise ArchiveError('未知备份类型，整包拒绝')
    if not isinstance(manifest['sequence'],int) or manifest['sequence']<1:raise ArchiveError('备份序号非法，整包拒绝')
    if manifest['kind']=='full' and manifest['sequence']!=1:raise ArchiveError('全量包序号必须为1，整包拒绝')
    if manifest['kind']=='incremental' and (manifest['sequence']<=1 or not manifest['parent_id']):raise ArchiveError('增量包链信息非法，整包拒绝')
    expected=digest(canonical(manifest)+b'.'+canonical(payload))
    if not isinstance(doc.get('signature'),str) or not _same(expected,doc['signature']):raise ArchiveError('签名校验失败：归档包内容已被篡改，整包拒绝')
    for t in TYPES:
        if not isinstance(payload.get(t+'s'),list):raise ArchiveError(f'载荷缺少 {t} 列表，整包拒绝')
    indexed={(x['type'],x['name']):x['content_hash'] for x in manifest['objects']}
    if len(indexed)!=len(manifest['objects']):raise ArchiveError('清单对象索引存在重复条目，整包拒绝')
    seen=set()
    for t,rows in ((t,payload[t+'s']) for t in TYPES):
        for row in rows:
            if not isinstance(row,dict) or 'name' not in row:raise ArchiveError(f'{t} 对象结构非法，整包拒绝')
            key=(t,row['name'])
            if key in seen:raise ArchiveError(f'{t} 载荷存在同名重复对象，整包拒绝')
            seen.add(key)
            want=indexed.get(key)
            if want!=content_hash(t,row):raise ArchiveError(f'{t}「{row["name"]}」指纹与清单不符：内容被篡改，整包拒绝')
    if seen!=set(indexed):raise ArchiveError('清单与载荷对象集合不一致，整包拒绝')
    counts=manifest['counts']
    if counts!={'datasets':len(payload['datasets']),'configs':len(payload['configs']),'trials':len(payload['trials'])}:
        raise ArchiveError('对象计数与载荷不符，整包拒绝')
    return doc
def _same(a,b):
    return isinstance(b,str) and hmac.compare_digest(a,b.lower())
