#!/usr/bin/env node
const fs = require('fs');
const gitignore = fs
  .readFileSync('.gitignore')
  .toString()
  .split('\n')
  .filter(l => !l.match(/^dist\//))
fs.writeFileSync('.npmignore', [
  ...gitignore,
  'default.code-workspace',
  'scripts/ignoregen.js',
  'README.md',
  '__tests__/',
  'dist/__tests__',
  'jest.config.js',
  'tsconfig.json',
].join('\n'));
