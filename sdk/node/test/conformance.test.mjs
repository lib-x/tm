import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { encode, decode } from '../dist/index.js';
import { transcript } from '../dist/conformance.js';

const cli = fileURLToPath(new URL('../dist/conformance.js', import.meta.url));
const run = (args, input) => spawnSync(process.execPath, [cli, ...args], { input, encoding: 'utf8', timeout: 10000 });

test('CLI emit/verify handles fixed transcript as fields; CRLF and reordered protobuf tags accepted', () => {
  const emitted = run(['emit']);
  assert.equal(emitted.status, 0, emitted.stderr);
  assert.equal(emitted.stderr, '');
  const lines = emitted.stdout.trimEnd().split('\n');
  assert.equal(lines.length, 10);
  for (const line of lines) assert.match(line, /^[0-9a-f]+$/);
  const frames = lines.map((line) => decode(Buffer.from(line, 'hex')));
  assert.deepEqual(frames.map((v) => v.body.case), ['hello', 'welcome', 'open', 'openAck', 'data', 'data', 'window', 'window', 'window', 'window']);
  const checked = run(['verify'], lines.join('\r\n') + '\r\n');
  assert.equal(checked.status, 0, checked.stderr);
  assert.equal(checked.stdout, '');
  // Frame 4 OpenAck with the body first and stream_id last.
  lines[3] = '000000046a000801';
  assert.equal(run(['verify'], lines.join('\n')).status, 0);
});

test('CLI verify rejects wrong content, truncation, extra frames, nonhex and excessive lines', () => {
  const lines = transcript().map((value) => Buffer.from(encode(value)).toString('hex'));
  const badData = transcript(); badData[4].body.value.payload[0] = 0;
  const inputs = [
    lines.slice(1).join('\n'), [...lines, lines[0]].join('\n'),
    badData.map((v) => Buffer.from(encode(v)).toString('hex')).join('\n'),
    'zz\n', '0\n', '\n', '0'.repeat(262154) + '\n', '00000010aa\n',
  ];
  for (const input of inputs) {
    const result = run(['verify'], input);
    assert.equal(result.status, 1, input.slice(0, 60));
    assert.equal(result.stdout, '');
    assert.match(result.stderr, /tmp-conformance:/);
  }
  const invalid = run(['endpoint', 'wrong']);
  assert.equal(invalid.status, 1);
  assert.match(invalid.stderr, /usage:/);
});

test('CLI endpoint initiator/responder complete streaming cross-process exchange with pure hex output', async (t) => {
  const a = spawn(process.execPath, [cli, 'endpoint', 'initiator'], { stdio: ['pipe', 'pipe', 'pipe'] });
  const b = spawn(process.execPath, [cli, 'endpoint', 'responder'], { stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(() => { a.kill(); b.kill(); });
  const timer = setTimeout(() => { a.kill(); b.kill(); }, 10000);
  t.after(() => clearTimeout(timer));
  const outputs = [[], []];
  const diagnostics = ['', ''];
  const relay = (from, to, index) => {
    let partial = '';
    from.stderr.on('data', (data) => { diagnostics[index] += data; });
    from.stdout.setEncoding('utf8');
    from.stdout.on('data', (data) => {
      partial += data;
      while (partial.includes('\n')) {
        const end = partial.indexOf('\n');
        const line = partial.slice(0, end);
        partial = partial.slice(end + 1);
        assert.match(line, /^[0-9a-f]+$/);
        outputs[index].push(decode(Buffer.from(line, 'hex')));
        to.stdin.write(line + '\r\n');
      }
    });
    from.stdout.on('end', () => { assert.equal(partial, ''); });
    // Endpoints intentionally close after reciprocal GOAWAY; late EOF is normal.
    to.stdin.on('error', () => {});
  };
  relay(a, b, 0); relay(b, a, 1);
  const exited = (child) => new Promise((resolve, reject) => {
    child.once('error', reject); child.once('close', (code, signal) => resolve({ code, signal }));
  });
  const result = await Promise.all([exited(a), exited(b)]);
  assert.deepEqual(result, [{ code: 0, signal: null }, { code: 0, signal: null }], diagnostics.join('\n'));
  assert.deepEqual(diagnostics, ['', '']);
  assert.deepEqual(outputs[0].map((v) => v.body.case), ['hello', 'open', 'data', 'window', 'window', 'goAway']);
  assert.deepEqual(outputs[1].map((v) => v.body.case), ['welcome', 'openAck', 'window', 'window', 'data', 'goAway']);
  assert.equal(new TextDecoder().decode(outputs[0][2].body.value.payload), '你好 TMP');
  assert.equal(new TextDecoder().decode(outputs[1][4].body.value.payload), 'ok');
  assert.equal(outputs[0][3].body.value.credit, 2);
  assert.equal(outputs[1][2].body.value.credit, 10);
});

test('endpoint rejects wrong exchange content and premature EOF with stderr-only diagnostics', () => {
  const welcome = Buffer.from(encode(transcript()[1])).toString('hex');
  const result = run(['endpoint', 'initiator'], welcome + '\n');
  assert.equal(result.status, 1);
  assert.match(result.stderr, /EOF/);
  for (const line of result.stdout.trimEnd().split('\n')) assert.match(line, /^[0-9a-f]+$/);
});
