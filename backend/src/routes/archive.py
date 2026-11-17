from fastapi import APIRouter,Depends,UploadFile,File,Form
from fastapi.responses import FileResponse
from ..config.database import get_db
from ..services.auth import current_user,admin
from ..services import archive
router=APIRouter(prefix='/archives')
@router.get('/backups')
def backups(db=Depends(get_db),user=Depends(current_user)):return archive.list_backups(db)
@router.post('/backups',status_code=201)
def make_backup(kind:str=Form('full'),db=Depends(get_db),user=Depends(admin)):
    if kind not in ('full','incremental'):raise ValueError('kind 只能是 full 或 incremental')
    return archive.create_backup(db,kind)
@router.get('/backups/{package_id}/download')
def download(package_id:str,db=Depends(get_db),user=Depends(current_user)):
    path,file_name=archive.backup_file(db,package_id)
    return FileResponse(path,media_type='application/json',filename=file_name)
@router.get('/restores')
def restores(db=Depends(get_db),user=Depends(current_user)):return archive.list_jobs(db)
@router.post('/restores',status_code=202)
async def upload(file:UploadFile=File(...),db=Depends(get_db),user=Depends(admin)):
    raw=await file.read()
    if len(raw)>64*1024*1024:raise ValueError('归档包不能超过 64MB')
    return archive.save_upload(db,file.filename or 'upload.json',raw)
@router.post('/restores/{package_id}/apply')
def apply(package_id:str,db=Depends(get_db),user=Depends(admin)):
    return archive.apply_restore(db,package_id)
