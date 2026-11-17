from datetime import datetime
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,Integer,BigInteger,DateTime,UniqueConstraint,func
from ..config.database import Base
class ArchiveObject(Base):
    """内容寻址台账：工坊内每个自然对象的内容指纹只登记一次，重包恢复据此复用。"""
    __tablename__='archive_objects'
    id:Mapped[int]=mapped_column(primary_key=True)
    object_type:Mapped[str]=mapped_column(String(10))
    content_hash:Mapped[str]=mapped_column(String(64))
    local_id:Mapped[int]=mapped_column(Integer)
    __table_args__=(UniqueConstraint('object_type','content_hash',name='uq_archive_object_hash'),
                    UniqueConstraint('object_type','local_id',name='uq_archive_object_local'))
class BackupRecord(Base):
    """每包对应同一时点的快照；watermark 记录本包纳入对象的 id 上界，下包从其后继续。"""
    __tablename__='backup_records'
    id:Mapped[int]=mapped_column(primary_key=True)
    package_id:Mapped[str]=mapped_column(String(40),unique=True)
    kind:Mapped[str]=mapped_column(String(10))
    sequence:Mapped[int]=mapped_column(Integer,unique=True)
    parent_id:Mapped[str|None]=mapped_column(String(40),nullable=True)
    file_name:Mapped[str]=mapped_column(String(160))
    trial_watermark:Mapped[int]=mapped_column(BigInteger,default=0)
    dataset_watermark:Mapped[int]=mapped_column(BigInteger,default=0)
    config_watermark:Mapped[int]=mapped_column(BigInteger,default=0)
    trial_count:Mapped[int]=mapped_column(Integer,default=0)
    dataset_count:Mapped[int]=mapped_column(Integer,default=0)
    config_count:Mapped[int]=mapped_column(Integer,default=0)
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
class RestoreJob(Base):
    """恢复从 pending（待校验）到 available（可用）；rejected 表示整包拒绝，任何阶段都不留半截数据。"""
    __tablename__='restore_jobs'
    id:Mapped[int]=mapped_column(primary_key=True)
    package_id:Mapped[str]=mapped_column(String(40),unique=True)
    file_name:Mapped[str]=mapped_column(String(160))
    status:Mapped[str]=mapped_column(String(12),default='pending')
    kind:Mapped[str|None]=mapped_column(String(10),nullable=True)
    sequence:Mapped[int|None]=mapped_column(Integer,nullable=True)
    parent_id:Mapped[str|None]=mapped_column(String(40),nullable=True)
    detail:Mapped[str|None]=mapped_column(String(2000),nullable=True)
    restored_trials:Mapped[int]=mapped_column(Integer,default=0)
    restored_datasets:Mapped[int]=mapped_column(Integer,default=0)
    restored_configs:Mapped[int]=mapped_column(Integer,default=0)
    reused:Mapped[int]=mapped_column(Integer,default=0)
    renamed:Mapped[int]=mapped_column(Integer,default=0)
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
    updated_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now(),onupdate=func.now())
