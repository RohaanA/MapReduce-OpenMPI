// Structs used for passing data between processes in MPI
struct mapLoad {
    int lineStart;
    int lineEnd;
};
struct entry {
    char* key;
    char* value;
};
struct mapReceive {
    char key[10];
    char value[50];
};


// Hashmap Functions
int entry_compare(const void *a, const void *b, void *udata) {
    const struct entry *ea = a;
    const struct entry *eb = b;
    return strcmp(ea->key, eb->key);
}

void *hashmap_set_concat(struct hashmap *map, const char *key, const char *value) {
    char delim = '|';

    if (!key) {
        panic("key is null");
    }
    map->oom = false;
    if (map->count == map->growat) {
        if (!resize(map, map->nbuckets*2)) {
            map->oom = true;
            return NULL;
        }
    }

    // Create a temporary entry with the given key and value
    struct entry temp_entry = { .key = (char *)key, .value = (char *)value };
    
    struct bucket *entry = map->edata;
    entry->hash = get_hash(map, &temp_entry);
    entry->dib = 1;
    memcpy(bucket_item(entry), &temp_entry, map->elsize);
    
    size_t i = entry->hash & map->mask;
    for (;;) {
        struct bucket *bucket = bucket_at(map, i);
        if (bucket->dib == 0) {
            memcpy(bucket, entry, map->bucketsz);
            map->count++;
            return NULL;
        }
        if (entry->hash == bucket->hash && 
            map->compare(bucket_item(entry), bucket_item(bucket), 
                         map->udata) == 0)
        {
            // Concatenate the new value with the existing value
            size_t old_value_len = strlen(((struct entry *)bucket_item(bucket))->value);
            size_t new_value_len = strlen(value);
            size_t delimiter_len = 1;
            char *new_value = malloc(old_value_len + delimiter_len + new_value_len + 1);
            if (!new_value) {
                map->oom = true;
                return NULL;
            }
            memcpy(new_value, ((struct entry *)bucket_item(bucket))->value, old_value_len);
            new_value[old_value_len] = delim; // Add a delimiter between old and new values
            memcpy(new_value + old_value_len + delimiter_len, value, new_value_len);
            new_value[old_value_len + delimiter_len + new_value_len] = '\0';
            ((struct entry *)bucket_item(bucket))->value = new_value;
            return ((struct entry *)bucket_item(bucket))->value;
        }
        if (bucket->dib < entry->dib) {
            memcpy(map->spare, bucket, map->bucketsz);
            memcpy(bucket, entry, map->bucketsz);
            memcpy(entry, map->spare, map->bucketsz);
        }
        i = (i + 1) & map->mask;
        entry->dib += 1;
    }
}



bool entry_iter(const void *item, void *udata) {
    const struct entry *entry = item;
    printf("%s=%s\n", entry->key, entry->value);
    return true;
}

uint64_t entry_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct entry *entry = item;
    return hashmap_sip(entry->key, strlen(entry->key), seed0, seed1);
}

char* hashmap_to_json(struct hashmap* map) {
    size_t mem_size = 1024;
    char* json = malloc(mem_size);
    if (json == NULL) {
        fprintf(stderr, "Failed to allocate memory for JSON string.\n");
        return NULL;
    }

    size_t json_size = snprintf(json, mem_size, "{");
    if (json_size >= mem_size) {
        mem_size = json_size + 1;
        char* tmp = realloc(json, mem_size);
        if (tmp == NULL) {
            fprintf(stderr, "Failed to reallocate memory for JSON string.\n");
            free(json);
            return NULL;
        }
        json = tmp;
    }

    size_t remaining_size = mem_size - json_size;
    size_t current_pos = json_size;

    size_t iter = 1;
    void* item;
    while (hashmap_iter(map, &iter, &item)) {
        const struct entry* entry_row = item;
        size_t key_len = strlen(entry_row->key);
        size_t value_len = strlen(entry_row->value);

        if (current_pos + key_len + value_len + 7 >= mem_size) {
            mem_size *= 2;
            char* tmp = realloc(json, mem_size);
            if (tmp == NULL) {
                fprintf(stderr, "Failed to reallocate memory for JSON string.\n");
                free(json);
                return NULL;
            }
            json = tmp;
            remaining_size = mem_size - current_pos;
        }

        json_size += snprintf(json + json_size, remaining_size, "\"%s\":\"%s\",", entry_row->key, entry_row->value);
        remaining_size = mem_size - json_size;
        current_pos = json_size;
    }

    json_size--; // Remove the last comma
    if (current_pos + 2 >= mem_size) {
        mem_size++;
        char* tmp = realloc(json, mem_size);
        if (tmp == NULL) {
            fprintf(stderr, "Failed to reallocate memory for JSON string.\n");
            free(json);
            return NULL;
        }
        json = tmp;
    }

    json_size += snprintf(json + json_size, remaining_size, "}");

    char* tmp = realloc(json, json_size + 1);
    if (tmp == NULL) {
        fprintf(stderr, "Failed to reallocate memory for JSON string.\n");
        free(json);
        return NULL;
    }
    json = tmp;
    json[json_size] = '\0';

    return json;
}
