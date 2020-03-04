# %%

import os, pymysql, json, time
import Confs.conf as cf
import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import \
    FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.store import SimpleFSDirectory

# Lucene Begin
print('lucene : ' + str(lucene.VERSION))
lucene.initVM(vmargs=['-Djava.awt.headless=true', '-Xss256K', '-Xms128m', '-Xmx3G'])

'''

def newFieldType(stored=True, tokenized=True, indexopt=IndexOptions.DOCS_AND_FREQS_AND_POSITIONS):
    nft = FieldType()
    nft.setStored(stored)
    nft.setTokenized(tokenized)
    nft.setIndexOptions(indexopt)
    return nft

def parse(file):
    try:
        g = rdflib.Graph()
        g.parse(file, format="xml")
        return ';'.join([','.join(stmt) for stmt in g])

        # with open(file, 'r', encoding='utf8') as f:
        #     return f.read()
    except:
        return ''

class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)

'''

# Constants

TF = [False, True]
IndOpt = [IndexOptions.DOCS, IndexOptions.DOCS_AND_FREQS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
          IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, IndexOptions.NONE]



def gen_new_field_type(para):
    # print(para)
    ft = FieldType()
    ft.setStored(TF[para[0]])
    ft.setTokenized(TF[para[1]])
    ft.setIndexOptions(IndOpt[para[2]])
    return ft

class IndexFiles():
    """Usage: python IndexFiles <doc_directory>"""

    def __init__(self, storeDir, commit_limit, analyzer=StandardAnalyzer()):
        if not os.path.exists(storeDir):
            os.mkdir(storeDir)

        self.store = SimpleFSDirectory(Paths.get(storeDir))
        self.analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        self.writer = IndexWriter(self.store, config)
        self.commit_cnt = 0
        self.commit_limit = commit_limit

    def check_commit(self, force=False):
        self.commit_cnt += 1
        if self.commit_cnt >= self.commit_limit or force:
            self.writer.commit
            self.commit_cnt = 0

    def write_close(self):
        self.writer.close()

    def addIndex(self, inst):
        doc = Document()
        for cont in inst:
            doc.add(Field(cont[0], cont[1], gen_new_field_type(cont[2])))
        self.writer.addDocument(doc)
        self.check_commit()

class Database_Reader():
    def __init__(self, datasetbase):
        self.dbid = datasetbase
        self.db = pymysql.connect(cf.db_ipaddress, cf.db_username, cf.db_password, cf.db_dbname)
        self.map_id2label()
        self.id2triple = None
        self.map_id2triple()

    def map_id2label(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM uri_label_id{};".format(self.dbid))
        datas = cursor.fetchall()

        id2label = {}
        desc = {}
        for id, val in enumerate(cursor.description):
            desc[val[0]] = id
        for line in datas:
            id2label[int(line[desc['id']])] = line[desc['label']]

        self.id2label = id2label

        print(time.strftime("%H:%M:%S"), end=' : ')
        print('id2label complete')

    def get_triple_from_id(self, id):

        # With self.id2triple pre stored
        if None != self.id2triple:
            if id not in self.id2triple.keys():
                return ''
            else:
                return self.id2triple[id]

        # Do Database Query
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM triple{} WHERE dataset_local_id={};".format(self.dbid, id))
        datas = cursor.fetchall()
        desc = {}
        for id, val in enumerate(cursor.description):
            desc[val[0]] = id
        result = []
        for line in datas:
            sub = int(line[desc['subject']])
            pre = int(line[desc['predicate']])
            obj = int(line[desc['object']])
            result.append(','.join([sub, pre, obj]))
        return ';'.join(result)

    def map_id2triple(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM triple{};".format(self.dbid))
        datas = cursor.fetchall()

        id2triple = {}
        desc = {}
        for id, val in enumerate(cursor.description):
            desc[val[0]] = id
        for line in datas:
            sub = int(line[desc['subject']])
            pre = int(line[desc['predicate']])
            obj = int(line[desc['object']])
            localid = int(line[desc['dataset_local_id']])
            if localid not in id2triple.keys():
                id2triple[localid] = []
            id2triple[localid].append((sub, pre, obj))

        self.id2triple = id2triple

        print(time.strftime("%H:%M:%S"), end=' : ')
        print('id2triple complete')

    def generate_contents(self, local_id):
        if None == local_id:
            return ''
        return ';'.join([ ','.join([self.id2label[id] for id in triple]) for triple in self.get_triple_from_id(int(local_id)) ])

    def commit_contents(self, index:IndexFiles):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM dataset{};".format(self.dbid))
        datas = cursor.fetchall()
        desc = {}
        for id, val in enumerate(cursor.description):
            desc[val[0]] = id
        with open(cf.info_file, 'r', encoding='utf8') as f:
            infos = json.load(f)
        # print(infos)
        for line in datas:
            dat = []
            for nam, typ in infos.items():
                cont = line[desc[nam]]
                if None == cont:
                    cont = ''
                dat.append((nam, cont, typ))
            local_id = line[desc['local_id']]
            dat.append(('content', self.generate_contents(local_id), [1, 1, 2]))

            index.addIndex(dat)

        print(time.strftime("%H:%M:%S"), end=' : ')
        print('get contents complete')

    def closedb(self):
        self.db.close()


# Begin Main

print(time.strftime("%H:%M:%S") + ' : begin')

# Initialize Class
ind = IndexFiles(storeDir=cf.store_path, commit_limit=1)
DB = Database_Reader(3)

# Do Commition
DB.commit_contents(ind)

# Close
DB.closedb()
ind.check_commit(True)
ind.write_close()

print(time.strftime("%H:%M:%S") + ' : done')


