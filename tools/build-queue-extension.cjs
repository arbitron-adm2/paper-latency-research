#!/usr/bin/env node
'use strict';
// Package the current extension source; never read historical inputs or overwrite a build.
const fs=require('node:fs'),path=require('node:path');
const root=path.resolve(__dirname,'..'),src=path.join(root,'apps/paper-queue-latency'),dest=path.join(root,'dist/paper-queue-latency');
if(fs.existsSync(dest))throw new Error('Build exists; choose a new output directory');
fs.mkdirSync(dest,{recursive:true});
for(const name of ['manifest.json','diagnostics.html','src'])fs.cpSync(path.join(src,name),path.join(dest,name),{recursive:true,errorOnExist:true});
console.log('Extension packaged in dist/paper-queue-latency');
