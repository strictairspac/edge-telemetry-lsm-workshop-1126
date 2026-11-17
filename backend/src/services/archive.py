"""试验档案的备份与恢复。

备份：全量包+依赖前置包的增量包，单事务快照保证每包同一时点；备份期间新创建的试验留给下一包。
恢复：待校验→可用/拒绝，全部对象在单事务内一同可见；内容指纹台账保证中断重试不重复新增。
"""
import os,threading
from sqlalchemy import select,func
from fastapi import HTTPException
from ..models.dataset import Dataset
from ..models.config import Config
from ..models.trial import Trial
from ..models.archive import ArchiveObject,BackupRecord,RestoreJob
from .config import serialize as config_row
from . import archive_package as pkg
ARCHIVE_DIR=os.getenv('ARCHIVE_DIR','/artifacts/archives')
_backup_lock=threading.RLock();_restore_lock=threading.RLock()
class AdvisoryLock:
    """MySQL 用命名行锁串行化同库操作；SQLite 等回退到进程内可重入锁。

    命名锁绑定物理连接，因此进入时固定同一 DBAPI 连接并在退出时于该连接释放，
    避免事务提交后连接归还连接池、释放动作落到别的连接。
    """
    def __init__(self,session,key):self.s=session;self.key=key;self.held=False;self._dbapi=None
    def __enter__(self):
        if self.s.bind.dialect.name=='mysql':
            self._dbapi=self.s.connection().connection
            cur=self._dbapi.cursor();cur.execute('SELECT GET_LOCK(%s,30)',(self.key,));got=cur.fetchone()[0];cur.close()
            if not int(got or 0):raise HTTPException(429,'另一个归档操作正在执行，请稍后重试')
        else:self._local().__enter__()
        self.held=True;return self
    def __exit__(self,*exc):
        if not self.held:return
        if self.s.bind.dialect.name=='mysql':
            cur=self._dbapi.cursor();cur.execute('SELECT RELEASE_LOCK(%s)',(self.key,));cur.close()
        else:self._local().__exit__(*exc)
    def _local(self):return _backup_lock if self.key.endswith('backup') else _restore_lock
# ---------- 内容指纹台账 ----------
def _known_hash(db,otype,chash):
    return db.scalar(select(ArchiveObject.local_id).where(ArchiveObject.object_type==otype,ArchiveObject.content_hash==chash))
def _register(db,otype,chash,local_id):db.add(ArchiveObject(object_type=otype,content_hash=chash,local_id=local_id))
def _serialized_dataset(x):return {'name':x.name,'description':x.description,'entries':x.entries}
def _serialized_config(x):return config_row(x)
def _serialized_trial(db,x):
    dataset=db.get(Dataset,x.dataset_id);config=db.get(Config,x.config_id)
    return {'name':x.name,'dataset_name':dataset.name if dataset else f'#{x.dataset_id}',
            'config_name':config.name if config else f'#{x.config_id}','status':x.status,
            'input_snapshot':x.input_snapshot,'result':x.result,'created_at':str(x.created_at)}
def _row_of(otype,obj,db):
    if otype=='dataset':return _serialized_dataset(obj)
    if otype=='config':return _serialized_config(obj)
    return _serialized_trial(db,obj)
# ---------- 备份 ----------
def create_backup(db,kind):
    os.makedirs(ARCHIVE_DIR,exist_ok=True)
    with AdvisoryLock(db,'lsm_archive_backup'):
        last=db.scalar(select(BackupRecord).order_by(BackupRecord.sequence.desc()))
        if kind=='full':
            if last:raise HTTPException(409,'全量备份已存在；后续变化请制作增量包，保持单一时点链')
            sequence=1
        else:
            if not last:raise HTTPException(409,'还没有全量包，无法制作增量包')
            sequence=last.sequence+1
        ds_wm,cfg_wm,tr_wm=(last.dataset_watermark,last.config_watermark,last.trial_watermark) if last else (0,0,0)
        datasets=[_serialized_dataset(x) for x in db.scalars(select(Dataset).where(Dataset.id>ds_wm).order_by(Dataset.id))]
        configs=[_serialized_config(x) for x in db.scalars(select(Config).where(Config.id>cfg_wm).order_by(Config.id))]
        # 只归档已完成试验；快照后仍在排队/运行的留给下一包。
        # 是否入包只由 id 水位线决定（每对象全链恰好一次），内容台账仅用于恢复判重，不跳过备份。
        trials=[_serialized_trial(db,x) for x in db.scalars(
            select(Trial).where(Trial.id>tr_wm,Trial.result.is_not(None)).order_by(Trial.id))]
        if kind=='incremental' and not (datasets or configs or trials):
            raise HTTPException(409,'自上一包以来没有新增对象，无需生成增量包')
        # 水位线只推进到本包实际纳入对象的最大 id；未完成试验等未纳入对象的 id 不越过水位线，留给后续包
        # 批次/配置无状态过滤，取全表最大；试验只取已完成且在旧水位线之后的最大 id
        new_ds_wm=db.scalar(select(func.coalesce(func.max(Dataset.id),0)))
        new_cfg_wm=db.scalar(select(func.coalesce(func.max(Config.id),0)))
        included_trials=db.scalars(select(Trial.id).where(Trial.id>tr_wm,Trial.result.is_not(None)).order_by(Trial.id)).all()
        new_tr_wm=max([tr_wm]+list(included_trials))
        watermarks={'trial':new_tr_wm,'dataset':new_ds_wm,'config':new_cfg_wm}
        doc=pkg.build_package(kind,sequence,last.package_id if last else None,datasets,configs,trials,watermarks)
        file_name=f"lsm-archive-{sequence:04d}-{kind}.json";path=os.path.join(ARCHIVE_DIR,file_name)
        with open(path,'wb') as f:f.write(pkg.encode(doc))
        local_by_type={'dataset':{x.name:x.id for x in db.scalars(select(Dataset))},
                       'config':{x.name:x.id for x in db.scalars(select(Config))},
                       'trial':{x.name:x.id for x in db.scalars(select(Trial))}}
        for otype,rows in (('dataset',datasets),('config',configs),('trial',trials)):
            for row in rows:
                chash=pkg.content_hash(otype,row)
                if not _known_hash(db,otype,chash):_register(db,otype,chash,local_by_type[otype][row['name']])
        db.add(BackupRecord(package_id=doc['manifest']['package_id'],kind=kind,sequence=sequence,
                            parent_id=doc['manifest']['parent_id'],file_name=file_name,
                            trial_watermark=new_tr_wm,dataset_watermark=new_ds_wm,config_watermark=new_cfg_wm,
                            trial_count=len(trials),dataset_count=len(datasets),config_count=len(configs)))
        db.commit()
        return {'file_name':file_name,'path':path,'record':_backup_dict(db.scalar(select(BackupRecord).where(BackupRecord.package_id==doc['manifest']['package_id'])))}
def list_backups(db):
    return [_backup_dict(x) for x in db.scalars(select(BackupRecord).order_by(BackupRecord.sequence))]
def _backup_dict(x):
    return {'id':x.id,'package_id':x.package_id,'kind':x.kind,'sequence':x.sequence,'parent_id':x.parent_id,
            'file_name':x.file_name,'counts':{'datasets':x.dataset_count,'configs':x.config_count,'trials':x.trial_count},
            'watermarks':{'dataset':x.dataset_watermark,'config':x.config_watermark,'trial':x.trial_watermark},
            'created_at':str(x.created_at)}
def backup_file(db,package_id):
    x=db.scalar(select(BackupRecord).where(BackupRecord.package_id==package_id))
    if not x:raise HTTPException(404,'归档包不存在')
    path=os.path.join(ARCHIVE_DIR,x.file_name)
    if not os.path.exists(path):raise HTTPException(404,'归档文件已从磁盘移除')
    return path,x.file_name
# ---------- 恢复 ----------
def save_upload(db,file_name,raw):
    doc=pkg.verify(raw)  # 上传时先做单包防伪验签，链与引用在应用阶段再判
    package_id=doc['manifest']['package_id']
    existing=db.scalar(select(RestoreJob).where(RestoreJob.package_id==package_id))
    if existing:
        # 已可用/待校验/恢复中的包不重复登记；被整包拒绝的包允许重新上传（例如补齐祖先包后）回到待校验
        if existing.status!='rejected':
            raise HTTPException(409,'该归档包已上传，恢复任务保持原状态，未重复登记')
        existing.status='pending';existing.detail='已重新上传，等待重新校验'
        existing.file_name=file_name;existing.kind=doc['manifest']['kind']
        existing.sequence=doc['manifest']['sequence'];existing.parent_id=doc['manifest']['parent_id']
        os.makedirs(ARCHIVE_DIR,exist_ok=True)
        with open(os.path.join(ARCHIVE_DIR,f"restore-{package_id}.json"),'wb') as f:f.write(raw)
        db.commit();db.refresh(existing);return _job_dict(existing)
    os.makedirs(ARCHIVE_DIR,exist_ok=True)
    safe=f"restore-{package_id}.json";path=os.path.join(ARCHIVE_DIR,safe)
    with open(path,'wb') as f:f.write(raw)
    m=doc['manifest']
    job=RestoreJob(package_id=package_id,file_name=file_name,status='pending',kind=m['kind'],
                   sequence=m['sequence'],parent_id=m['parent_id'])
    db.add(job);db.commit();db.refresh(job)
    return _job_dict(job)
def list_jobs(db):
    return [_job_dict(x) for x in db.scalars(select(RestoreJob).order_by(RestoreJob.id.desc()))]
def recover_interrupted(db):
    """进程重启后把卡在 applying 的任务退回待校验，保证中断可继续且不会半套可见。"""
    for x in db.scalars(select(RestoreJob).where(RestoreJob.status=='applying')):
        x.status='pending';x.detail='上次恢复在写入阶段中断，已退回待校验，可重试'
    db.commit()
def apply_restore(db,package_id,fail_after=None):
    with AdvisoryLock(db,'lsm_archive_restore'):
        job=db.scalar(select(RestoreJob).where(RestoreJob.package_id==package_id))
        if not job:raise HTTPException(404,'恢复任务不存在')
        if job.status in ('available','rejected'):return _job_dict(job)  # 重试不重复新增
        job.status='applying';db.commit()
        try:
            summary=_validate_and_apply(db,job,fail_after)
            job.status='available';job.detail='全部对象校验通过并一同可见'
            for k,v in summary.items():setattr(job,k,v)
            db.commit()
        except pkg.ArchiveError as exc:
            db.rollback();job=db.scalar(select(RestoreJob).where(RestoreJob.package_id==package_id))
            job.status='rejected';job.detail=str(exc);db.commit()
            raise HTTPException(422,str(exc))
        except Exception:
            db.rollback();job=db.scalar(select(RestoreJob).where(RestoreJob.package_id==package_id))
            job.status='pending';job.detail='写入中断，已保留待校验状态，重试不会重复新增';db.commit();raise
        return _job_dict(db.scalar(select(RestoreJob).where(RestoreJob.package_id==package_id)))
def _load_job_package(db,job):
    path=os.path.join(ARCHIVE_DIR,f"restore-{job.package_id}.json")
    if not os.path.exists(path):raise pkg.ArchiveError('归档文件缺失，整包拒绝')
    with open(path,'rb') as f:return pkg.verify(f.read())
def _validate_and_apply(db,job,fail_after):
    doc=_load_job_package(db,job);m=doc['manifest'];payload=doc['payload']
    # 祖先链：全量必须是1号；增量的祖先须已可用且逐包衔接，缺祖先整包拒绝
    chain={j.package_id:j for j in db.scalars(select(RestoreJob))}
    if m['kind']=='full':
        if m['parent_id'] is not None:raise pkg.ArchiveError('全量包不能携带前置包，整包拒绝')
    else:
        parent=chain.get(m['parent_id'])
        if not parent or parent.status!='available':raise pkg.ArchiveError('祖先包缺失或尚未可用，整包拒绝')
        if parent.sequence!=m['sequence']-1:raise pkg.ArchiveError('增量包与祖先包序号不连续，整包拒绝')
    # 汇总链上所有可用包与本包，形成跨包引用解析域
    docs=[doc]
    if m['kind']=='incremental':
        pid=m['parent_id']
        while pid:
            pj=chain.get(pid)
            if not pj or pj.status!='available':raise pkg.ArchiveError('祖先链断裂，整包拒绝')
            pdoc=_load_job_package(db,pj)
            docs.append(pdoc);pid=pdoc['manifest']['parent_id']
    ref_domain={}
    for d in docs:
        for t in pkg.TYPES:
            for row in d['payload'][t+'s']:ref_domain[(t,row['name'])]=row
    # 引用完整性：每个试验引用的批次与配置必须在本包或祖先包内可解析，缺失即整包拒绝
    for t in payload['trials']:
        if ('dataset',t['dataset_name']) not in ref_domain:raise pkg.ArchiveError(f"试验「{t['name']}」引用的批次「{t['dataset_name']}」缺失，整包拒绝")
        if ('config',t['config_name']) not in ref_domain:raise pkg.ArchiveError(f"试验「{t['name']}」引用的配置「{t['config_name']}」缺失，整包拒绝")
    stats={'restored_datasets':0,'restored_configs':0,'restored_trials':0,'reused':0,'renamed':0}
    inserted=[]
    def free_name(model,base):
        name=base;suffix=0
        while db.scalar(select(model.id).where(model.name==name)):
            suffix+=1;name=f"{base} (恢复 {job.sequence}-{job.package_id[:6]}-{suffix})"
        return name,suffix
    def place(otype,row,model,build):
        chash=pkg.content_hash(otype,row)
        local_id=_known_hash(db,otype,chash)
        if local_id:stats['reused']+=1;return db.get(model,local_id)
        # 台账未登记时，按同名同内容复用目标工坊已有对象，并补登台账
        same_name=db.scalar(select(model).where(model.name==row['name']))
        if same_name is not None and pkg.content_hash(otype,_row_of(otype,same_name,db))==chash:
            _register(db,otype,chash,same_name.id);stats['reused']+=1;return same_name
        name,suffix=free_name(model,row['name']);renamed=suffix>0
        obj=build(name,row);db.add(obj);db.flush()
        if renamed:stats['renamed']+=1
        _register(db,otype,chash,obj.id);inserted.append((otype,chash,obj.id))
        stats['restored_'+({'dataset':'datasets','config':'configs','trial':'trials'}[otype])]+=1
        if fail_after and len(inserted)>=fail_after:raise RuntimeError('注入写入中断')
        return obj
    dataset_map={};config_map={}
    for row in payload['datasets']:
        dataset_map[row['name']]=place('dataset',row,Dataset,lambda n,r:Dataset(name=n,description=r['description'],entries=r['entries']))
    for row in payload['configs']:
        config_map[row['name']]=place('config',row,Config,lambda n,r:Config(name=n,memtable_limit=r['memtable_limit'],l0_trigger=r['l0_trigger'],max_levels=r['max_levels'],final_compaction=r['final_compaction']))
    # 本包未携带、但试验引用的批次/配置，在链域中按内容复用或另存
    def resolve_ref(otype,name,model,build):
        row=ref_domain[(otype,name)];chash=pkg.content_hash(otype,row)
        local_id=_known_hash(db,otype,chash)
        if local_id:return db.get(model,local_id)
        existing=db.scalar(select(model).where(model.name==name))
        if existing is not None and pkg.content_hash(otype,_row_of(otype,existing,db))==chash:
            _register(db,otype,chash,existing.id);return existing  # 同名同内容复用
        new_name=free_name(model,name)[0]  # 同名异内容或重名冲突：另存，不动原有对象
        obj=build(new_name,row);db.add(obj);db.flush();stats['renamed']+=1
        _register(db,otype,chash,obj.id)
        return obj
    for row in payload['trials']:
        ds=dataset_map.get(row['dataset_name']) or resolve_ref('dataset',row['dataset_name'],Dataset,lambda n,r:Dataset(name=n,description=r['description'],entries=r['entries']))
        cf=config_map.get(row['config_name']) or resolve_ref('config',row['config_name'],Config,lambda n,r:Config(name=n,memtable_limit=r['memtable_limit'],l0_trigger=r['l0_trigger'],max_levels=r['max_levels'],final_compaction=r['final_compaction']))
        def make_trial(n,r,ds=ds,cf=cf):
            return Trial(name=n,dataset_id=ds.id,config_id=cf.id,status=r['status'],input_snapshot=r['input_snapshot'],result=r['result'])
        place('trial',row,Trial,make_trial)
    db.commit()  # 全部对象一同可见；此前任何异常都整体回滚
    return stats
def _job_dict(x):
    return {'id':x.id,'package_id':x.package_id,'file_name':x.file_name,'status':x.status,'kind':x.kind,
            'sequence':x.sequence,'parent_id':x.parent_id,'detail':x.detail,
            'restored':{'datasets':x.restored_datasets,'configs':x.restored_configs,'trials':x.restored_trials},
            'reused':x.reused,'renamed':x.renamed,'created_at':str(x.created_at),'updated_at':str(x.updated_at)}
