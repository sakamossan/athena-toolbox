#!/usr/bin/env node
const fs = require('fs');
const gitignore = fs.readFileSync('.gitignore').toString().split('\n');
fs.writeFileSync('.npmignore', [
  ...gitignore,
  'default.code-workspace',
  'README.md',
  '__tests__/',
  'src/',
  'jest.config.js',
].join('\n'));