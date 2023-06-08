import sqlite3
import pandas as pd
import hashlib
import logging
import os

from glob import glob
from tqdm.contrib.concurrent import process_map


def get_hash(text):
    hash_object = hashlib.sha256()
    hash_object.update(text.encode())
    hex_dig = hash_object.hexdigest()
    return hex_dig


class CreateHash:
    """Create hash for each text in a sqlite database.
    Save to parquet"""

    def __init__(self, dbpath, target_dir="."):        
        self.dbpath = dbpath
        self.name = self.get_name()
        self.urns = self.get_urns()

        df = self.iter_texts()
        df.to_parquet(os.path.join(target_dir, self.name + ".parquet"))


    def get_name(self):
        name = os.path.basename(self.dbpath)
        name = os.path.splitext(name)[0]
        return name

    def get_urns(self):
        query = "SELECT urn FROM urns"
        with sqlite3.connect(self.dbpath) as conn:
            urns = pd.read_sql(query, conn)

        return urns.urn

    def get_text(self, urn):
        query = "SELECT word FROM ft where urn = ?"
        with sqlite3.connect(self.dbpath) as conn:
            df = pd.read_sql(query, conn, params=(str(urn),))
        text = "".join(df.word)
        text_hash = get_hash(text)
        return text_hash
    
    def iter_texts(self):        
        lst = list()        
        for urn in self.urns:
            
            dct = dict()
            text_hash = self.get_text(urn)
            print(urn, text_hash)
            dct = {"urn" : urn, "hash" : text_hash}
            lst.append(dct)

        df = pd.DataFrame(lst)

        assert len(df) != 0, "text has no words"
        
        return df
    


def main():
    db_path = ("/home/larsm/projects/oppdatere_dhlab_dbs/dhlab_text_hashing/data/alto_300195000_300219999.db")
    print("Running")
    CreateHash(dbpath=db_path)
    print("done!")

if __name__=="__main__":
    main()