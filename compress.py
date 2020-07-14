# coding:utf-8

import re
import six
import codecs
import pickle
import functools
import heapq as hq
from pathlib import Path
from collections import defaultdict

# should be directory contains the files ending with '.txt'
data_dir = '10_July/'
# save path for compressed file, need to determine further
save_path = '10_July/compressed/{}.bin'
compressed_file_dir = '10_July/compressed/'
# recover path
recover_path = '10_July/{}.txt'
# Huffman Tree save path
huffman_tree_path = '10_July/huffman_tree.pt'

def checkObject(func):
  @functools.wraps(func)
  def checkObjectInner(*args, **kwargs):
    if type(args[0]) == type(args[1]):
      return func(*args, **kwargs)
    else:
      raise TypeError
  return checkObjectInner

class Node(object):
  def __init__(self, weight, data=None):
    self.weight = weight
    self.data = data
    self.p_left = None
    self.p_right = None

  @checkObject
  def __lt__(self, other):
    return self.weight < other.weight
  
  @checkObject
  def __eq__(self, other):
    return self.weight == other.weight

  def __str__(self):
    return str(self.data)  

class HuffmanTree(object):
  def __init__(self):
    self.root = None
    self.code_scheme = {}

  def buildTree(self, word_freq):
    # Initial a heap
    heap = []
    for word, freq in word_freq.items():
      hq.heappush(heap, Node(freq, word))

    while len(heap) > 1:
      node_a = hq.heappop(heap)
      node_b = hq.heappop(heap)
      node_meta = Node(node_a.weight + node_b.weight, None)
      node_meta.p_left = node_a
      node_meta.p_right = node_b
      hq.heappush(heap, node_meta)
    
    assert len(heap) == 1
    self.root = heap[0]
    self.getCode()

  def getCode(self):
    if self.root is None:
      raise ValueError('Huffman Tree has not been created.')
    self._getCode(self.root.p_left, '0')
    self._getCode(self.root.p_right, '1')
    return self.code_scheme
  
  def _getCode(self, node, prefix):
    if node.data is not None:
      self.code_scheme[node.data] = prefix
    else:
      self._getCode(node.p_left, prefix + '0')
      self._getCode(node.p_right, prefix + '1')
  
  def encode(self, string):
    if len(self.code_scheme) == 0:
      raise ValueError('Huffman Code has not been built.')
    string = string.strip()
    return ''.join([self.code_scheme[w] for w in string])
  
  def decode(self, code):
    def _check(node):
      if node.data is not None:
        return True
      else:
        return False

    cur_node = self.root
    string = ''
    for cur_c in code:
      cur_c = int(cur_c)
      if cur_c == 0:
        cur_node = cur_node.p_left
        if _check(cur_node):
          string += cur_node.data
          cur_node = self.root
      else:
        cur_node = cur_node.p_right
        if _check(cur_node):
          string += cur_node.data
          cur_node = self.root
      string = re.sub('α', '\n', string)
    return string

def countFrequency(dir_path):
  data_list = Path(dir_path).rglob('*.txt')
  word_frequency = defaultdict(int)
  for path in data_list:
    with codecs.open(path, 'r', 'utf-8') as file:
      for line in file:
        for v in line:
          word_frequency[v] +=1
          word_frequency['α'] +=1
  return word_frequency

def encodeFile(huffman_tree, file_path, save_path):
  code = []
  with codecs.open(file_path, 'r', 'utf-8') as file:
    for line in file:
      line = line.strip() + 'α'
      code.append(huffman_tree.encode(line))
  code = ''.join(code)
  code_padding = 8 - len(code) % 8
  code += '0' * code_padding  

  with codecs.open(save_path, 'wb') as file:
    file.write(six.int2byte(code_padding))
    for i in range(len(code) // 8):
      file.write(six.int2byte(int(code[i*8 : i*8+8], 2)))
    file.flush()

def decodeFile(huffman_tree, file_path, save_path):
  with codecs.open(file_path, 'rb') as file:
    data = file.read()

  code_padding = data[0]
  
  code = ''
  for number in data[1:]:
    # '0b' is always in front of the binary code, eg. '0b10010001',
    # however, it will ignore the front 0, eg. '0b00111011'.
    code += '0' * (10 - len(bin(number))) + bin(number)[2:]
  code = code[:-code_padding]

  with codecs.open(save_path, 'w', 'utf-8') as file_2:
    line_decoded = huffman_tree.decode(code)
    to_write = line_decoded
    file_2.write(to_write)
    file_2.flush()

if __name__ == '__main__':
  # count word frequency
  word_frequency = countFrequency(data_dir)
  # build huffman tree
  huffman_tree = HuffmanTree()
  huffman_tree.buildTree(word_frequency)

  # compress file
  for path in Path(data_dir).rglob('*.txt'):
    s_path = str(path).split('/')[-1]
    s_path = save_path.format(s_path[:-4])
    encodeFile(huffman_tree, path, s_path)

  # save huffman tree
  with codecs.open(huffman_tree_path, 'wb') as file:
    pickle.dump(huffman_tree, file)

  # # load huffman tree
  # with codecs.open(huffman_tree_path, 'rb') as file:
  #   huffman_tree = pickle.load(file)

  # # recover the file
  # for path in Path(compressed_file_dir).rglob('*.bin'):
  #   r_path = str(path).split('/')[-1]
  #   r_path = recover_path.format(r_path[:-4])
  #   decodeFile(huffman_tree, path, r_path)