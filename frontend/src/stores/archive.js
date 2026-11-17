import{listBackups,listRestores}from'../api/archive.js';
export const archiveStore={
  backups:[],restores:[],
  async load(){const [b,r]=await Promise.all([listBackups(),listRestores()]);this.backups=b;this.restores=r;return this}
};
