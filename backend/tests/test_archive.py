"""备份与恢复端到端测试：SQLite + TestClient，无需 Docker。

覆盖：全量/增量时点快照、备份期间并发创建、跨包引用、目标工坊同名复用/异内容另存、
缺祖先/篡改/引用缺失整包拒绝、待校验→可用状态机、写入中断后恢复且重试不重复新增、
全部对象一同可见、恢复期间原有业务仍可操作、角色权限。
"""
import json,os,tempfile,sys,threading
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..'))
import pytest
from sqlalchemy import create_engine,select
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
@pytest.fixture()
def env(tmp_path,monkeypatch):
    monkeypatch.setenv('DATABASE_URL',f"sqlite:///{tmp_path/'test.db'}")
    monkeypatch.setenv('JWT_SECRET','test-secret')
    monkeypatch.setenv('SEED_DEMO','false')
    monkeypatch.setenv('ARCHIVE_DIR',str(tmp_path/'archives'))
    # 隔离已导入模块，确保每个用例独立建库
    for name in list(sys.modules):
        if name.startswith('src.'):del sys.modules[name]
    from src.config.database import Base,engine,Session
    from src.models import user,dataset,config,trial,audit,archive as archive_model
    from src.services import trial as trial_service
    class FakeQueue:
        def __init__(self):self.jobs=[]
        def rpush(self,k,v):self.jobs.append(v)
        def ping(self):return True
    trial_service.queue=FakeQueue()
    Base.metadata.create_all(engine)
    from src.main import app
    from src.utils.password import hash_password
    from src.models.user import User
    with Session() as db:
        for role in ('admin','operator','viewer'):
            db.add(User(email=role+'@beiling.test',role=role,password_hash=hash_password('pw123456')))
        db.commit()
    client=TestClient(app)
    def token(role):
        r=client.post('/auth/login',json={'email':role+'@beiling.test','password':'pw123456'})
        return r.json()['token']
    yield {'client':client,'token':token,'Session':Session,'tmp':tmp_path,
           'models':{'Dataset':dataset.Dataset,'Config':config.Config,'Trial':trial.Trial,
                     'RestoreJob':archive_model.RestoreJob,'BackupRecord':archive_model.BackupRecord,
                     'ArchiveObject':archive_model.ArchiveObject}}
    engine.dispose()
AUTH=lambda env,role:{'Authorization':'Bearer '+env['token'](role)}
def make_dataset(client,token,name,entries=None):
    entries=entries or [{'op':'put','key':'k1','value':'v1'},{'op':'delete','key':'k2'}]
    r=client.post('/datasets',json={'name':name,'description':'d','entries':entries},headers=AUTH(token,'admin') if False else {'Authorization':'Bearer '+token})
    assert r.status_code==201,r.text;return r.json()
def make_config(client,token,name,**kw):
    payload={'name':name,'memtable_limit':2,'l0_trigger':2,'max_levels':3,'final_compaction':True,**kw}
    r=client.post('/configs',json=payload,headers={'Authorization':'Bearer '+token})
    assert r.status_code==201,r.text;return r.json()
def complete_trial(env,client,token,name,ds_id,cfg_id):
    r=client.post('/trials',json={'name':name,'dataset_id':ds_id,'config_id':cfg_id},headers={'Authorization':'Bearer '+token})
    assert r.status_code==202,r.text
    tid=r.json()['id']
    # 无 worker/redis：直接跑确定性引擎并落库，模拟试验已完成
    from src.models.trial import Trial
    from src.services.lsm_engine import simulate
    Session=env['Session']
    with Session() as db:
        t=db.get(Trial,tid);t.result=simulate(t.input_snapshot['entries'],t.input_snapshot['config']);t.status='passed';db.commit()
    return tid
def backup(client,token,kind):
    r=client.post('/archives/backups',data={'kind':kind},headers={'Authorization':'Bearer '+token})
    assert r.status_code==201,r.text;return r.json()
def download(env,client,token,pid):
    r=client.get(f'/archives/backups/{pid}/download',headers={'Authorization':'Bearer '+token})
    assert r.status_code==200;return r.content
def upload(client,token,raw,filename='p.json'):
    r=client.post('/archives/restores',files={'file':(filename,raw,'application/json')},headers={'Authorization':'Bearer '+token})
    return r
def apply(client,token,pid):
    return client.post(f'/archives/restores/{pid}/apply',headers={'Authorization':'Bearer '+token})
# ---------- 场景 ----------
def test_full_backup_snapshot_and_incremental_chain(env):
    c,t=env['client'],env['token']('admin')
    # 没有全量时不能做增量
    r=c.post('/archives/backups',data={'kind':'incremental'},headers=AUTH(env,'admin'))
    assert r.status_code==409
    ds=make_dataset(c,t,'批次甲');cfg=make_config(c,t,'配置甲')
    tid=complete_trial(env,c,t,'试验甲',ds['id'],cfg['id'])
    b1=backup(c,t,'full');assert b1['record']['counts']['trials']==1
    raw1=download(env,c,t,b1['record']['package_id'])
    doc=json.loads(raw1);assert doc['manifest']['kind']=='full' and doc['manifest']['sequence']==1
    assert len(doc['payload']['trials'])==1 and doc['payload']['trials'][0]['result']
    # 增量包只含新增，且携带 parent
    ds2=make_dataset(c,t,'批次乙');cfg2=make_config(c,t,'配置乙')
    complete_trial(env,c,t,'试验乙',ds2['id'],cfg2['id'])
    b2=backup(c,t,'incremental');rec=b2['record']
    assert rec['kind']=='incremental' and rec['parent_id']==b1['record']['package_id']
    raw2=download(env,c,t,rec['package_id']);d2=json.loads(raw2)
    assert [x['name'] for x in d2['payload']['trials']]==['试验乙']
    assert [x['name'] for x in d2['payload']['datasets']]==['批次乙']
    # 单链内重复全量拒绝；没有新增时增量也拒绝
    assert c.post('/archives/backups',data={'kind':'full'},headers=AUTH(env,'admin')).status_code==409
    assert c.post('/archives/backups',data={'kind':'incremental'},headers=AUTH(env,'admin')).status_code==409
def test_concurrent_creation_during_backup_goes_to_next_package(env):
    c,t=env['client'],env['token']('admin')
    ds=make_dataset(c,t,'批次并发');cfg=make_config(c,t,'配置并发')
    complete_trial(env,c,t,'既有试验',ds['id'],cfg['id'])
    from src.models.trial import Trial
    Session=env['Session']
    # 模拟备份读快照之后、提交之前，另一个请求创建了新试验（未完成）
    r=c.post('/trials',json={'name':'备份中新建','dataset_id':ds['id'],'config_id':cfg['id']},headers={'Authorization':'Bearer '+t})
    assert r.status_code==202
    b1=backup(c,t,'full')
    with Session() as db:names=[x.name for x in db.scalars(select(Trial))]
    assert '备份中新建' in names  # 备份期间业务仍可创建
    raw=download(env,c,t,b1['record']['package_id']);d=json.loads(raw)
    assert [x['name'] for x in d['payload']['trials']]==['既有试验']  # 新试验留给下一包
    # 新试验完成后进入下一包（即使 id 与既有 id 穿插）
    with Session() as db:x=db.scalar(select(Trial).where(Trial.name=='备份中新建'));nid=x.id
    from src.services.lsm_engine import simulate
    with Session() as db:
        x=db.get(Trial,nid);x.result=simulate(x.input_snapshot['entries'],x.input_snapshot['config']);x.status='passed';db.commit()
    b2=backup(c,t,'incremental')
    d2=json.loads(download(env,c,t,b2['record']['package_id']))
    assert [x['name'] for x in d2['payload']['trials']]==['备份中新建']
def test_cross_package_restore_and_remap(env):
    c,t=env['client'],env['token']('admin')
    ds=make_dataset(c,t,'共享批次');cfg=make_config(c,t,'共享配置')
    complete_trial(env,c,t,'第一包试验',ds['id'],cfg['id'])
    raw1=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    # 第二包新试验继续引用第一包的批次与配置（跨包引用）
    complete_trial(env,c,t,'第二包试验',ds['id'],cfg['id'])
    raw2=download(env,c,t,backup(c,t,'incremental')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    r1=upload(tc,tt,raw1);assert r1.status_code==202
    j1=r1.json();assert j1['status']=='pending'  # 待校验
    a1=apply(tc,tt,j1['package_id']);assert a1.status_code==200 and a1.json()['status']=='available'
    # 目标工坊恢复后试验可用，引用已重映射
    trials=tc.get('/trials',headers=AUTH(target,'admin')).json()
    assert len(trials)==1 and trials[0]['result'] and trials[0]['status']=='passed'
    # 增量包本身不带共享批次/配置，靠祖先链解析跨包引用
    j2=upload(tc,tt,raw2).json();a2=apply(tc,tt,j2['package_id']).json()
    assert a2['status']=='available' and a2['restored']['trials']==1
    assert tc.get('/datasets',headers=AUTH(target,'admin')).json().__len__()==1  # 引用复用，未重复建批次
    t2=tc.get('/trials',headers=AUTH(target,'admin')).json()
    assert {x['name'] for x in t2}=={'第一包试验','第二包试验'}
    # 全部试验的引用都有效
    for x in t2:assert x['dataset_id'] and x['config_id']
def test_same_name_same_content_reuse_diff_content_rename(env):
    c,t=env['client'],env['token']('admin')
    make_dataset(c,t,'重名批次',entries=[{'op':'put','key':'a','value':'1'}])
    cfg=make_config(c,t,'重名配置')
    ds_id=c.get('/datasets',headers={'Authorization':'Bearer '+t}).json()[0]['id']
    complete_trial(env,c,t,'重名试验',ds_id,cfg['id'])
    raw=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    # 目标已有同名同内容批次 → 复用；同名异内容配置 → 另存；同名试验 → 另存且原试验保留
    tds=make_dataset(tc,tt,'重名批次',entries=[{'op':'put','key':'a','value':'1'}])
    make_config(tc,tt,'重名配置',memtable_limit=4)  # 同名异内容
    # 目标工坊原有一个同名但内容不同的试验
    tcfg=tc.get('/configs',headers={'Authorization':'Bearer '+tt}).json()[0]
    complete_trial(target,tc,tt,'重名试验',tds['id'],tcfg['id'])
    j=upload(tc,tt,raw).json();res=apply(tc,tt,j['package_id']).json()
    assert res['status']=='available' and res['reused']>=1 and res['renamed']>=1
    dss=tc.get('/datasets',headers=AUTH(target,'admin')).json()
    assert len(dss)==1  # 同内容复用
    cfgs=tc.get('/configs',headers=AUTH(target,'admin')).json()
    assert len(cfgs)==2 and any('恢复' in x['name'] for x in cfgs)
    trials=tc.get('/trials',headers=AUTH(target,'admin')).json()
    names=[x['name'] for x in trials]
    assert len(trials)==2 and '重名试验' in names and any('恢复' in n for n in names)  # 原试验保留，恢复的另存
def test_tampered_and_missing_ancestor_and_missing_reference_rejected(env):
    c,t=env['client'],env['token']('admin')
    ds=make_dataset(c,t,'批次X');cfg=make_config(c,t,'配置X')
    complete_trial(env,c,t,'试验X',ds['id'],cfg['id'])
    raw1=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    complete_trial(env,c,t,'试验Y',ds['id'],cfg['id'])
    raw2=download(env,c,t,backup(c,t,'incremental')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    # 1) 篡改签名/内容 → 上传即拒绝
    tampered=bytearray(raw1);tampered[-15]^=0x01
    assert upload(tc,tt,bytes(tampered)).status_code==422
    # 2) 篡改载荷后重打包但不更新签名 → 应用拒绝
    doc=json.loads(raw1);doc['payload']['trials'][0]['name']='被改名'
    bad=json.dumps(doc,ensure_ascii=False).encode()
    r=upload(tc,tt,bad);assert r.status_code==422 and '篡改' in r.json()['detail']
    # 3) 缺祖先：直接恢复增量包
    j2=upload(tc,tt,raw2).json()
    r=apply(tc,tt,j2['package_id']);assert r.status_code==422 and '祖先' in r.json()['detail']
    job=tc.get('/archives/restores',headers=AUTH(target,'admin')).json()[0]
    assert job['status']=='rejected'  # 整包拒绝且无残留对象
    assert tc.get('/trials',headers=AUTH(target,'admin')).json()==[]
    # 4) 引用缺失：手工构造一个引用不存在批次的全量包
    from src.services import archive_package as ap
    broken=ap.encode(ap.build_package('full',1,None,[],[],[
        {'name':'孤儿试验','dataset_name':'幽灵批次','config_name':'配置X','status':'passed',
         'input_snapshot':{'entries':[],'config':{}},'result':{'verified':True}}],
        {'dataset':1,'config':1,'trial':1}))
    j3=upload(tc,tt,broken).json()
    r=apply(tc,tt,j3['package_id']);assert r.status_code==422 and '缺失' in r.json()['detail']
def test_full_package_keeps_referenced_dependencies_closed(env):
    c,t=env['client'],env['token']('admin')
    # 正常全量包：试验引用的批次与配置随包携带，恢复后引用完整
    ds=make_dataset(c,t,'封闭批次');cfg=make_config(c,t,'封闭配置')
    complete_trial(env,c,t,'封闭试验',ds['id'],cfg['id'])
    raw=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    doc=json.loads(raw)
    assert {x['name'] for x in doc['payload']['datasets']}=={'封闭批次'}
    assert {x['name'] for x in doc['payload']['configs']}=={'封闭配置'}
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    j=upload(tc,tt,raw).json();res=apply(tc,tt,j['package_id']).json()
    assert res['status']=='available'
    tr=tc.get('/trials',headers=AUTH(target,'admin')).json()[0]
    got_ds=tc.get('/datasets',headers=AUTH(target,'admin')).json()[0]
    got_cfg=tc.get('/configs',headers=AUTH(target,'admin')).json()[0]
    assert tr['dataset_id']==got_ds['id'] and tr['config_id']==got_cfg['id']  # 引用已重映射
def test_rejected_package_can_reupload_after_ancestor_ready(env):
    c,t=env['client'],env['token']('admin')
    ds=make_dataset(c,t,'链批次');cfg=make_config(c,t,'链配置')
    complete_trial(env,c,t,'链试验1',ds['id'],cfg['id'])
    raw1=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    complete_trial(env,c,t,'链试验2',ds['id'],cfg['id'])
    raw2=download(env,c,t,backup(c,t,'incremental')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    j2=upload(tc,tt,raw2).json()
    assert apply(tc,tt,j2['package_id']).status_code==422  # 缺祖先先拒绝
    # 祖先补齐
    j1=upload(tc,tt,raw1).json();assert apply(tc,tt,j1['package_id']).json()['status']=='available'
    # 同一增量包重新上传：不再 409，回到待校验后成功
    again=upload(tc,tt,raw2)
    assert again.status_code==202 and again.json()['status']=='pending'
    res=apply(tc,tt,j2['package_id']).json()
    assert res['status']=='available'
    assert {x['name'] for x in tc.get('/trials',headers=AUTH(target,'admin')).json()}=={'链试验1','链试验2'}
def test_interrupted_restore_resumes_without_duplicates(env):
    c,t=env['client'],env['token']('admin')
    for i in range(3):
        ds=make_dataset(c,t,f'中断批次{i}');cfg=make_config(c,t,f'中断配置{i}')
        complete_trial(env,c,t,f'中断试验{i}',ds['id'],cfg['id'])
    raw=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    from src.services import archive as svc
    from src.models.archive import RestoreJob
    Session=target['Session']
    j=upload(tc,tt,raw).json()
    # 注入：写入 2 个对象后中断
    with Session() as db:
        with pytest.raises(RuntimeError):svc.apply_restore(db,j['package_id'],fail_after=2)
    with Session() as db:job=db.scalar(select(RestoreJob).where(RestoreJob.package_id==j['package_id']))
    assert job.status=='pending'  # 中断后退回待校验，无半截数据对外可见
    # 模拟进程重启恢复
    with Session() as db:svc.recover_interrupted(db)
    # 重试成功且不重复新增
    res=apply(tc,tt,j['package_id']).json()
    assert res['status']=='available' and res['restored']['datasets']==3 and res['restored']['trials']==3
    # 再次重试：幂等，直接返回既有结果且对象不重复新增
    again=apply(tc,tt,j['package_id']).json()
    assert again['status']=='available'
    assert len(tc.get('/datasets',headers=AUTH(target,'admin')).json())==3
    assert len(tc.get('/configs',headers=AUTH(target,'admin')).json())==3
    assert len(tc.get('/trials',headers=AUTH(target,'admin')).json())==3
    # 台账中每类对象恰好 3 条，无重复登记
    from src.models.archive import ArchiveObject
    with Session() as db:
        assert db.query(ArchiveObject).filter(ArchiveObject.object_type=='trial').count()==3
        assert db.query(ArchiveObject).filter(ArchiveObject.object_type=='dataset').count()==3
def test_business_operates_during_pending_restore(env):
    c,t=env['client'],env['token']('admin')
    ds=make_dataset(c,t,'批次Z');cfg=make_config(c,t,'配置Z')
    complete_trial(env,c,t,'试验Z',ds['id'],cfg['id'])
    raw=download(env,c,t,backup(c,t,'full')['record']['package_id'])
    target=fresh_workshop(env);tc,tt=target['client'],target['token']('admin')
    j=upload(tc,tt,raw).json()  # 停在待校验
    # 待校验期间原有业务照常：登记批次、配置、试验
    nds=make_dataset(tc,tt,'本地新批次');ncfg=make_config(tc,tt,'本地新配置')
    r=tc.post('/trials',json={'name':'本地试验','dataset_id':nds['id'],'config_id':ncfg['id']},headers=AUTH(target,'admin'))
    assert r.status_code==202
    res=apply(tc,tt,j['package_id']).json()
    assert res['status']=='available'
    names={x['name'] for x in tc.get('/trials',headers=AUTH(target,'admin')).json()}
    assert {'试验Z','本地试验'}<=names  # 原有业务与恢复对象一同可见
def test_roles_and_visibility(env):
    c=env['client']
    # 备份/恢复写操作仅 admin
    r=c.post('/archives/backups',data={'kind':'full'},headers=AUTH(env,'operator'))
    assert r.status_code==403
    r=c.post('/archives/backups',data={'kind':'full'},headers=AUTH(env,'viewer'))
    assert r.status_code==403
    # 读接口登录用户均可
    assert c.get('/archives/backups',headers=AUTH(env,'viewer')).status_code==200
    assert c.get('/archives/restores',headers=AUTH(env,'operator')).status_code==200
    # 未登录被拒绝（HTTPBearer 缺凭据返回 403）
    assert c.get('/archives/backups').status_code in (401,403)
def fresh_workshop(env):
    """构造一个独立的空工坊（独立 SQLite 库），用于换机恢复演练。"""
    if not hasattr(fresh_workshop,'_n'):fresh_workshop._n=iter(range(100))
    new_path=env['tmp']/f"target-{next(fresh_workshop._n)}.db"
    engine=create_engine(f'sqlite:///{new_path}')
    Session=sessionmaker(engine,expire_on_commit=False)
    from src.config.database import Base
    from src.models.user import User
    from src.utils.password import hash_password
    # 复用同一 Base 的全部表定义
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            db.add(User(email=role+'@beiling.test',role=role,password_hash=hash_password('pw123456')))
        db.commit()
    # 临时把全局 engine/Session 指向新工坊，TestClient 的依赖即生效
    import src.config.database as real
    real.engine=engine;real.Session=Session
    from src.main import app
    from fastapi.testclient import TestClient
    client=TestClient(app)
    def token(role):return client.post('/auth/login',json={'email':role+'@beiling.test','password':'pw123456'}).json()['token']
    return {'client':client,'token':token,'Session':Session}
