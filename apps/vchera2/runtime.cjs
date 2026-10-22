'use strict';
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '.runtime');
function privateRuntime() {
  process.umask(0o077);
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const s = fs.lstatSync(root);
  if (!s.isDirectory() || s.isSymbolicLink() || s.uid !== process.getuid()) throw new Error('Unsafe runtime directory');
  fs.chmodSync(root, 0o700);
  return root;
}
function atomicPrivateJSON(file, value) {
  if (path.dirname(file) !== privateRuntime()) throw new Error('Write outside private runtime');
  const tmp = file + '.tmp';
  const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW, 0o600);
  try { fs.writeFileSync(fd, JSON.stringify(value, null, 2)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  fs.renameSync(tmp, file);
  fs.chmodSync(file, 0o600);
}
module.exports = { root, privateRuntime, atomicPrivateJSON };
