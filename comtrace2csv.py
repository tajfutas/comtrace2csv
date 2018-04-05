import collections
import datetime
import pathlib
import re


entry = collections.namedtuple('Entry',
    ('time', 'source', 'dest', 'bytes'))
instruction_entry = collections.namedtuple('InstructionEntry',
    ('times', 'source', 'dest', 'bytes'))
ENTRY_RESTR = (r'^(\d{4}/[01]\d/[0-3]\d '
    r'[0-2]\d:[0-5]\d:[0-5]\d\.\d+) '
    r'([^\r\n]+?)(<?)-\(trace\)-(>?)([^\r\n]*?): '
    r'MSG_LINE_DATA\s{\[(\d+)\]:(.*?)}$')
entry_reobj = re.compile(ENTRY_RESTR, re.MULTILINE | re.DOTALL)


class Instruction(tuple):

  def __new__(cls, cmd, data=b''):
    return super().__new__(cls, (int(cmd), bytes(data)))

  def __repr__(self):
    return f'Instruction({self.cmd}, {self.data})'

  @property
  def cmd(self):
    return self[0]

  @property
  def data(self):
    return self[1]


class InstructionConsumer:

  def __init__(self):
    self.cachebytes = collections.deque()
    try:
      next(self)
    except StopIteration:
      pass

  def __iter__(self):
    return self

  def __next__(self):
    self.pro = None
    self.state = '0'
    self.cmd = None
    self.length = None
    self.data = bytearray()
    self.crc = bytearray()
    self.instr = None
    self.instrbytes = bytearray()
    while True:
      try:
        byte = self.cachebytes.popleft()
      except IndexError:
        self.cachebytes.extend(self.instrbytes)
        raise StopIteration from None
      self.instrbytes.append(byte)
      if self.state in ('0',) and byte == 0xFF:
        self.state = 'PRO'
      elif self.state in ('0', 'PRO') and byte in (0x06, 0x15):
        self.pro = byte
        return self.pro
      elif self.state in ('0', 'PRO', 'CMD') and byte == 0x02:
        self.pro = byte
        self.state = 'CMD'
      elif self.state in ('0', 'PRO'):
        self.cachebytes.extendleft(self.instrbytes)
        self.instrbytes = bytearray()
        raise ValueError('invalid instruction byte')
      elif self.state in ('CMD',):
        self.cmd = byte
        if byte <= 0x1F or byte == 0xC4:
          self.state = 'LEGACYDATA'
        else:
          self.state = 'LEN'
      elif self.state in ('LEGACYDATA',):
        if byte == 0x10:
          self.state = 'DLE'
        elif byte == 0x03:
          return Instruction(self.cmd, self.data)
        else:
          self.data.append(byte)
      elif self.state in ('LEN',):
        self.length = byte
        if byte:
          self.state = 'DATA'
        else:
          self.state = 'CRC'
      elif self.state in ('DLE',):
        if byte > 0x1F:
          self.cachebytes.extendleft(self.instrbytes)
          self.instrbytes = bytearray()
          raise ValueError('invalid instruction byte')
        self.data.append(byte)
        self.state = 'LEGACYDATA'
      elif self.state in ('DATA',):
        self.data.append(byte)
        if len(self.data) == self.length:
          self.state = 'CRC'
      elif self.state in ('CRC',):
        self.crc.append(byte)
        if len(self.crc) == 2:
          self.state = 'ETX'
      elif self.state in ('ETX'):
        if byte != 0x03:
          self.cachebytes.extendleft(self.instrbytes)
          self.instrbytes = bytearray()
          raise ValueError('invalid instruction byte')
        return Instruction(self.cmd, self.data)

  def clean(self):
    pro = self.pro
    state = self.state
    cmd = self.cmd
    length = self.length
    data = self.data
    crc = self.crc
    instr = self.instr
    instrbytes = self.instrbytes
    cleanedbytes = bytearray()
    while True:
      try:
        next(self)
      except StopIteration:
        break
      except ValueError:
        cleanedbytes.append(self.cachebytes.popleft())
      else:
        self.cachebytes.extendleft(self.instrbytes)
    self.pro = pro
    self.state = state
    self.cmd = cmd
    self.length = length
    self.data = data
    self.crc = crc
    self.instr = instr
    self.instrbytes = instrbytes
    return cleanedbytes

  def quietsend(self, newbytes):
    self.cachebytes.extend(newbytes)

  def send(self, newbytes):
    self.quietsend(newbytes)
    return next(self)


# http://stackoverflow.com/a/312464/2334951
def chunks(seq, n):
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def entry_iter(tracestr):
  for m in entry_reobj.finditer(tracestr):
    groups = m.groups()
    dt = datetime.datetime.strptime(groups[0] + '000',
        '%Y/%m/%d %H:%M:%S.%f')
    if groups[2] and not groups[3]:
      nodes = groups[4], groups[1]
    elif groups[3] and not groups[2]:
      nodes = groups[1], groups[4]
    else:
      s = m.string[slice(*m.span())]
      raise TypeError('Invalid format: {}'.format(s[:48]))
    dlen = int(groups[5])
    dtext = ' '.join(s[3:50].rstrip()
        for s in chunks(groups[6], 72))
    data = bytes(int(s[:2],16) for s in chunks(dtext, 3))
    assert dlen == len(data)
    yield entry(dt, *nodes, data)


def instruction_iter(tracestr):
  def coroutine():
    d_comms = collections.defaultdict(collections.deque)
    d_instrcons = collections.defaultdict(InstructionConsumer)
    while True:
      entry = yield
      if not entry:
        continue
      time, source, dest, entry_bytes = entry
      dkey = source, dest
      d_comms[dkey].append((time, entry_bytes))
      instrcons = d_instrcons[dkey]
      instrcons.quietsend(entry_bytes)
      for i, instr in enumerate(instrcons):
        times = d_comms[dkey][0][0], d_comms[dkey][-1][0]
        data = instrcons.instrbytes
        yield instruction_entry(times, *dkey, data)
        if instrcons.cachebytes:
          d_comms[dkey] = collections.deque([d_comms[dkey][-1]])
        else:
          del d_comms[dkey]
  subgen = entry_iter(tracestr)
  subcoro = coroutine()
  for entry in subgen:
    next(subcoro)
    instruction = subcoro.send(entry)
    if instruction:
      yield instruction


def glob_instruction_iter(globstr):
  p_iter = pathlib.Path().glob(globstr)
  for p in sorted(p_iter):
    with p.open() as f:
      yield from instruction_iter(f.read())


if __name__ == '__main__':

  import csv
  import sys

  show_usage = False

  if not (3 <= len(sys.argv) <= 4):
    show_usage = True
  else:
    globstr = sys.argv[1]
    if not list(pathlib.Path().glob(globstr)):
      show_usage = True

  if show_usage:
    print('USAGE: comtrace2csv.py <logfile(s)> <outfile>')
    sys.exit()
  else:
    giit = glob_instruction_iter(globstr)
    instructions = sorted(giit)
    with open(sys.argv[2], 'w', newline='') as csvfile:
      writer = csv.writer(csvfile)
      for i in instructions:
        datastr = ' '.join('{:0>2X}'.format(x) for x in i.bytes)
        writer.writerow(i.times + i[1:-1] + (datastr,))
