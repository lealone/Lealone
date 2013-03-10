/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

#include <jni.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "com_yahoo_omid_tso_CommitHashMap.h"

#define MAX_KEY_SIZE 256
/**
 * The load factor for the hashtable.
 */
long largestOrder = 1;
/*
 * keep statistics of the # of average memory access
 */
long totalwalkforget = 0;
long totalwalkforput = 0;
long totalget = 0;
long totalput = 0;
long gmaxCommits = 0;
/**
 * The total number of entries in the hash table.
 */
int count = 0;
struct Entry;
struct LargeEntry;
struct StartCommit;
//the hash map
LargeEntry (*table);
StartCommit (*commitTable);



int tableLength;
/**
 * An entry could be garbage collected if its older than this threshold  
 * (The value of this field is (int)(capacity * loadFactor).)
 */

int threshold;

/*
 * Class:     com_yahoo_omid_CommitHashMap
 * Method:    gettotalput
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_gettotalput
(JNIEnv * env, jclass jcls) {
   return totalput;
}

/*
 * Class:     com_yahoo_omid_CommitHashMap
 * Method:    gettotalget
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_gettotalget
(JNIEnv * env, jclass jcls) {
   return totalget;
}

/*
 * Class:     com_yahoo_omid_CommitHashMap
 * Method:    gettotalwalkforput
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_gettotalwalkforput
(JNIEnv * env, jclass jcls) {
   return totalwalkforput;
}

/*
 * Class:     com_yahoo_omid_CommitHashMap
 * Method:    gettotalwalkforget
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_gettotalwalkforget
(JNIEnv * env, jclass jcls) {
   return totalwalkforget;
}

/*
 * Each item that stores the start timestamp and commit timestamp
 */
struct StartCommit {
   jlong start;//which is the start timestamp;
   jlong commit;//which is commit timestamp
};

/**
 * Innerclass that acts as a datastructure to create a new entry in the
 * table.
 * Avoid using object to use less memory walks
 */
struct Entry {
   long order;//the assigned order after insert
   int hash;//keep the computed hash for efficient comparison of keys
   //jbyte key[8];//which is row id;
   jbyte* key;//which is row id concatinated into table id
   jbyte rowidsize;//the key size = rowidsize + tableidsize
   jbyte tableidsize;//
   //jlong tag;//which is the start timestamp;
   jlong value;//which is commit timestamp
   //important must be the last field because of memcpy
   Entry* next;

   Entry() {
      order = 0;
      //hash = 0;
      key = NULL;
      //tag = 0;
      value = 0;
      next = NULL;
   }

   Entry(Entry* e) {
      *this = *e;
   }
};

struct LargeEntry {
   Entry e1;
   Entry e2;
   Entry e3;

   LargeEntry() {
      e1.next = &e2;
      e2.next = &e3;
   }
};

/*
 * Class:     CommitHashMap
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_omid_tso_CommitHashMap_init
(JNIEnv * env, jobject jobj, jint initialCapacity, jint maxCommits,jfloat loadFactor) {
   tableLength = initialCapacity;
   threshold = (int) (initialCapacity * loadFactor);
   printf("initialCapacity %d\n", initialCapacity);
   printf("Entry %lu Entry* %lu long %lu int %lu char %lu jbyte %lu jbyte* %lu jlong %lu\n", 
         sizeof(Entry), sizeof(Entry*), sizeof(long), sizeof(int), sizeof(char), sizeof(jbyte), sizeof(jbyte*), sizeof(jlong));
   table = new LargeEntry[initialCapacity];
   commitTable = new StartCommit[maxCommits];
   memset(commitTable, 0, sizeof(StartCommit)*maxCommits);
   gmaxCommits = maxCommits;

   //Initialze the table not to interfere with garbage collection
   printf("Entry* %p %%8 %lu ...\n", table, ((size_t)table)%8);
   printf("MEMORY initialization start ...\n");
   for (int i = 0; i < initialCapacity; i++) {
      if (i%1000000==0) {
         printf("MEMORY i=%d\n", i);
         fflush(stdout);
      }
      //Entry* e2 = new Entry();
      //Entry* e3 = new Entry();
      //table[i].next = e2;
      //e2->next = e3;
   }
   printf("MEMORY initialization end\n");
   fflush(stdout);
}


// Returns the value to which the specified key is mapped in this map.
// If there are multiple values with the same key, return the first
// The first is the one with the largest key, because (i) put always
// put the recent ones ahead, (ii) a new put on the same key has always
// larger value (because value is commit timestamp and the map is atmoic)
//
// @param   key   a key in the hashtable.
// @return  the value to which the key is mapped in this hashtable;
//          <code>NULL</code> if the key is not mapped to any value in
//          this hashtable.

/*
 * Class:     CommitHashMap
 * Method:    get
 * Signature: (JI)J
 */

jbyte keyarray[MAX_KEY_SIZE];
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_get
(JNIEnv * env , jobject jobj, jbyteArray rowId, jbyteArray tableId, jint hash) {
   totalget++;
   jsize rowidsize  = env->GetArrayLength(rowId);
   jsize tableidsize  = env->GetArrayLength(tableId);
   env->GetByteArrayRegion(rowId,0,rowidsize,keyarray);
   env->GetByteArrayRegion(tableId,0,tableidsize,keyarray + rowidsize * sizeof(jbyte));
   char keyarraysize = (rowidsize + tableidsize) * sizeof(jbyte);
   int index = (hash & 0x7FFFFFFF) % tableLength;
   for (Entry* e = &(table[index].e1); e != NULL; e = e->next) {
      totalwalkforget++;
      if (e->order == 0)//empty
         break;
      if (e->hash == hash && e->rowidsize == rowidsize && e->tableidsize == tableidsize)
         if (memcmp(e->key, keyarray, keyarraysize)==0)
            return e->value;
   }
   return 0;
}

/*
 * Class:     CommitHashMap
 * Method:    put
 * Signature: (JJJI)Z
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_put
(JNIEnv * env , jobject jobj, jbyteArray rowId, jbyteArray tableId, jlong value, jint hash, jlong largestDeletedTimestamp) {
   totalput++;
   int index = (hash & 0x7FFFFFFF) % tableLength;
   Entry* firstBucket = &(table[index].e1);
   bool keyarrayloaded = false;
   jsize rowidsize, tableidsize;
   unsigned int keyarraysize;

   Entry* lastEntry = NULL;//after the loop, it points to the last entry
   for (Entry* e = firstBucket; e != NULL; lastEntry = e, e = e->next) {
      totalwalkforput++;

      bool isOld = e->order == 0 ? 
         true :
         largestOrder - e->order > threshold;
      if (isOld) {
         if (e->value > largestDeletedTimestamp) {
            largestDeletedTimestamp = e->value;
         }

         if (keyarrayloaded == false) {
            rowidsize  = env->GetArrayLength(rowId);
            tableidsize  = env->GetArrayLength(tableId);
         }
         if (e->key == NULL || (e->rowidsize + e->tableidsize) < (rowidsize + tableidsize)) {//not reusable 
            free(e->key);
            //jbyte* key = (jbyte *)malloc(len * sizeof(jbyte));
            e->key = (jbyte *)malloc((rowidsize + tableidsize) * sizeof(jbyte));
         }
         if (keyarrayloaded == false) {
            env->GetByteArrayRegion(rowId,0,rowidsize,e->key);
            env->GetByteArrayRegion(tableId,0,tableidsize,e->key + rowidsize * sizeof(jbyte));
         }
         else memcpy(e->key, keyarray, keyarraysize);

         e->rowidsize = rowidsize;
         e->tableidsize = tableidsize;
         e->hash = hash;
         //e->tag = tag;
         e->value = value;
         e->order = ++largestOrder;
         return largestDeletedTimestamp;
      }

      if (keyarrayloaded == false) {
         rowidsize  = env->GetArrayLength(rowId);
         tableidsize  = env->GetArrayLength(tableId);
         keyarraysize = (rowidsize + tableidsize) * sizeof(jbyte);
         env->GetByteArrayRegion(rowId,0,rowidsize,keyarray);
         env->GetByteArrayRegion(tableId,0,tableidsize,keyarray + rowidsize * sizeof(jbyte));
         keyarrayloaded = true;
      }
      if (e->hash == hash && e->rowidsize == rowidsize && e->tableidsize == tableidsize) {
         if (memcmp(e->key, keyarray, keyarraysize)==0)  {
            //e->tag = tag;
            e->value = value;
            e->order = ++largestOrder;
            return largestDeletedTimestamp;
         }
      }
   }

   //printf("new entry");
   // Creates the new entry.
   LargeEntry* le = new LargeEntry();
   Entry* newentry = &(le->e1);
   lastEntry->next = newentry;
   if (keyarrayloaded == false) {
      rowidsize  = env->GetArrayLength(rowId);
      tableidsize  = env->GetArrayLength(tableId);
   }
   newentry->key = (jbyte *)malloc((rowidsize + tableidsize) * sizeof(jbyte));
   if (keyarrayloaded == false) {
      env->GetByteArrayRegion(rowId,0,rowidsize,newentry->key);
      env->GetByteArrayRegion(tableId,0,tableidsize,newentry->key + rowidsize * sizeof(jbyte));
   }
   else memcpy(newentry->key, keyarray, keyarraysize);
   newentry->rowidsize = rowidsize;
   newentry->tableidsize = tableidsize;
   newentry->hash = hash;
   //newentry->tag = tag;
   newentry->value = value;
   newentry->order = ++largestOrder;
   //newentry->next = e;
   if (count % 100000 == 0) {
      printf("NNNNNNNNNNNNNNNNNNNew Entry %d\n" , count);
      fflush(stdout);
   }
   count++;
   return largestDeletedTimestamp;
}


JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_getCommittedTimestamp(JNIEnv *, jobject, jlong startTimestamp) {
   int key = startTimestamp % gmaxCommits;
   StartCommit& entry = commitTable[key];
   if (entry.start == startTimestamp)
      return entry.commit;
   return 0;//which means that there is not such entry in the array, either deleted or never entered
}

JNIEXPORT jlong JNICALL Java_com_yahoo_omid_tso_CommitHashMap_setCommitted(JNIEnv * env , jobject jobj, jlong startTimestamp, jlong commitTimestamp, jlong largestDeletedTimestamp) {
   int key = startTimestamp % gmaxCommits;
   StartCommit& entry = commitTable[key];
   //assume(entry.start != startTimestamp);
   if (entry.start != startTimestamp && entry.commit > largestDeletedTimestamp)
      largestDeletedTimestamp = entry.commit;
   entry.start = startTimestamp;
   entry.commit = commitTimestamp;
   return largestDeletedTimestamp;
}


