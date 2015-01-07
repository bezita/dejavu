
import redis

from dejavu.database import Database


class SQLDatabase(Database):
    """
    Queries:

    1) Find duplicates (shouldn't be any, though):

        select `hash`, `song_id`, `offset`, count(*) cnt
        from fingerprints
        group by `hash`, `song_id`, `offset`
        having cnt > 1
        order by cnt asc;

    2) Get number of hashes by song:

        select song_id, song_name, count(song_id) as num
        from fingerprints
        natural join songs
        group by song_id
        order by count(song_id) desc;

    3) get hashes with highest number of collisions

        select
            hash,
            count(distinct song_id) as n
        from fingerprints
        group by `hash`
        order by n DESC;

    => 26 different songs with same fingerprint (392 times):

        select songs.song_name, fingerprints.offset
        from fingerprints natural join songs
        where fingerprints.hash = "08d3c833b71c60a7b620322ac0c0aba7bf5a3e73";
    """

    type = "redis"

    # tables
    FINGERPRINTS_TABLENAME = "fingerprints"
    SONGS_TABLENAME = "songs"
    COUNTER_TABLENAME = "counter"

    # fields
    FIELD_HASH = "hash"
    FIELD_SONG_ID = "song_id"
    FIELD_OFFSET = "offset"
    FIELD_SONGNAME = "song_name"
    FIELD_FINGERPRINTED = "fingerprinted"

    def __init__(self, **options):
        super(SQLDatabase, self).__init__()
        #self.cursor = cursor_factory(**options)
        self._options = options
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.cur = redis.Redis(connection_pool=pool)

    def after_fork(self):
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        pass #Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.

        This also removes all songs that have been added but have no
        fingerprints associated with them.
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.CREATE_SONGS_TABLE)
            #cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            #cur.execute(self.DELETE_UNFINGERPRINTED)

    def empty(self):
        """
        Drops tables created by dejavu and then creates them again
        by calling `SQLDatabase.setup`.

        .. warning:
            This will result in a loss of data
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.DROP_FINGERPRINTS)
            #cur.execute(self.DROP_SONGS)

        #self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Removes all songs that have no fingerprints associated with them.
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_songs(self):
        """
        Returns number of songs the database has fingerprinted.
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.SELECT_UNIQUE_SONG_IDS)

            #for count, in cur:
            #    return count
            #return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints the database has fingerprinted.
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.SELECT_NUM_FINGERPRINTS)

            #for count, in cur:
            #    return count
            #return 0

    def set_song_fingerprinted(self, sid):
        """
        Set the fingerprinted flag to TRUE (1) once a song has been completely
        fingerprinted in the database.
        """
        songname = self.cur.get("%s:%d"%(self.SONGS_TABLENAME,sid))
        self.cur.sadd("%s:song_saved"%(self.COUNTER_TABLENAME),songname)

    def get_songs(self):
        """
        Return songs that have the fingerprinted flag set TRUE (1).
        """
        songs = self.cur.smembers("%s:song_saved"%(self.COUNTER_TABLENAME))
        row = {}
        for a in songs:
			row[self.FIELD_SONGNAME] = a
			yield row


    def get_song_by_id(self, sid):
        """
        Returns song by its ID.
        """
        songname = self.cur.get("%s:%d"%(self.SONGS_TABLENAME,int(sid)))
        return {self.FIELD_SONGNAME:songname}

    def insert(self, hash, sid, offset):
        """
        Insert a (sha1, song_id, offset) row into database.
        """
        pass #with self.cursor() as cur:
            #cur.execute(self.INSERT_FINGERPRINT, (hash, sid, offset))

    def insert_song(self, songname):
        """
        Inserts song in the database and returns the ID of the inserted record.
        """
        sid = self.cur.incr("%s:songid"%(self.COUNTER_TABLENAME))
        self.cur.set("%s:%d"%(self.SONGS_TABLENAME,sid),songname)
        return sid

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        # select all if no key
        pass #query = self.SELECT_ALL if hash is None else self.SELECT

        #with self.cursor() as cur:
         #   cur.execute(query)
          #  for sid, offset in cur:
           #     yield (sid, offset)

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        pass #return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        pipe = self.cur.pipeline()
        counter = 0
        for hash,offset in hashes:
			counter += 1
			#print "%s:%s : %d %d"%(self.FINGERPRINTS_TABLENAME,hash.upper(),sid,offset)
			pipe.sadd("%s:%s"%(self.FINGERPRINTS_TABLENAME,hash.upper()),"%s:%d"%(sid,offset))
			if counter > 1000:
				pipe.execute()
				counter = 0
        if counter > 0:
			pipe.execute()

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hash, offset in hashes:
            mapper[hash.upper()] = offset

        # Get an iteratable of all the hashes we need
        values = mapper.keys()
        result = []
        tmp = []
        pipe = self.cur.pipeline()
        counter = 0
        for hash in values:
			counter += 1
			#print "hash: %s:%s"% (self.FINGERPRINTS_TABLENAME,hash)
			pipe.smembers("%s:%s"%(self.FINGERPRINTS_TABLENAME,hash))
			if counter > 1000:
				result.extend(pipe.execute())
				counter = 0
        if counter > 0:
			result.extend(pipe.execute())

        for hash in values:
			cr = result.pop(0)
			#print "%s: "%(hash)
			if len(cr) > 0:				
				for sid_hash in cr:
					fr = sid_hash.split(':')
					#print "%s : %s" % (fr[0],fr[1])
					yield (fr[0],int(fr[1]) - mapper[hash])

    def __getstate__(self):
        pass #return (self._options,)

    def __setstate__(self, state):
        pass #self._options, = state
        #self.cursor = cursor_factory(**self._options)
