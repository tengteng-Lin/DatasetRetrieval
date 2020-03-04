# %%

import sys
import Confs.conf as cf
import lucene
from pprint import pprint
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexReader, DirectoryReader
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.analysis.cjk import CJKAnalyzer
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser

print(lucene.VERSION)
lucene.initVM(vmargs=['-Djava.awt.headless=true', '-Xss256K', '-Xms128m', '-Xmx3G'])

class Searcher():
    def __init__(self, index_dir, result_max=100):
        indir = Paths.get(index_dir)
        indir = SimpleFSDirectory(indir)
        self.indir = DirectoryReader.open(indir)
        self.analyzer = StandardAnalyzer()
        self.searcher = IndexSearcher(self.indir)
        self.result_max = result_max

    def Query(self, query):
        my_query = QueryParser('notes', self.analyzer).parse(query)
        # print(my_query)
        total_hits = self.searcher.search(my_query, self.result_max)
        self.scoreDocs = total_hits.scoreDocs
        # return {hit.doc:self.searcher.doc(hit.doc).get("contents") for hit in total_hits.scoreDocs}

    def getResults(self, resdict):
        ans_set = []
        for hit in self.scoreDocs:
            ans = {}
            id = hit.doc
            content = {}
            for para in resdict:
                content[para] = self.searcher.doc(id).get(para)
            ans['id'] = id
            ans['content'] = content
            ans_set.append(ans)
        return ans_set

se = Searcher(cf.index_path)

se.Query('name:"ocean data" AND "sea floor"')
result = se.getResults(['name'])

pprint(result)
