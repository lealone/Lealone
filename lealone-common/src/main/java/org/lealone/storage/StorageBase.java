/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.db.Constants;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;

//子类通常只需要实现openMap方法
public abstract class StorageBase implements Storage {

    protected static final String TEMP_NAME_PREFIX = Constants.NAME_SEPARATOR + "temp"
            + Constants.NAME_SEPARATOR;

    protected final Map<StorageEventListener, StorageEventListener> listeners = new ConcurrentHashMap<>();
    protected final Map<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();
    protected final Map<String, Object> config;
    protected boolean closed;

    public StorageBase(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public void closeMap(String name) {
        StorageMap<?, ?> map = maps.remove(name);
        if (map != null)
            map.close();
    }

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public StorageMap<?, ?> getMap(String name) {
        return maps.get(name);
    }

    @Override
    public Set<String> getMapNames() {
        return new HashSet<>(maps.keySet());
    }

    @Override
    public String nextTemporaryMapName() {
        int i = 0;
        String name = null;
        while (true) {
            name = TEMP_NAME_PREFIX + i++;
            if (!maps.containsKey(name))
                return name;
        }
    }

    @Override
    public String getStoragePath() {
        return (String) config.get(StorageSetting.STORAGE_PATH.name());
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public long getDiskSpaceUsed() {
        long total = 0;
        for (StorageMap<?, ?> map : maps.values())
            total += map.getDiskSpaceUsed();
        return total;
    }

    @Override
    public long getMemorySpaceUsed() {
        long total = 0;
        for (StorageMap<?, ?> map : maps.values())
            total += map.getMemorySpaceUsed();
        return total;
    }

    @Override
    public void save() {
        for (StorageMap<?, ?> map : maps.values())
            map.save();
    }

    @Override
    public void drop() {
        close();
        if (!isInMemory())
            FileUtils.deleteRecursive(getStoragePath(), false);
    }

    @Override
    public void backupTo(String fileName, Long lastDate) {
        if (isInMemory())
            return;
        save();
        backupFiles(fileName, lastDate);
    }

    @Override
    public void close() {
        for (StorageEventListener listener : listeners.values())
            listener.beforeClose(this);
        listeners.clear();
        save();
        closeImmediately();
    }

    @Override
    public void closeImmediately() {
        closed = true;

        for (StorageMap<?, ?> map : maps.values())
            map.close();

        maps.clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void registerEventListener(StorageEventListener listener) {
        listeners.put(listener, listener);
    }

    @Override
    public void unregisterEventListener(StorageEventListener listener) {
        listeners.remove(listener);
    }

    protected InputStream getInputStream(String mapName, FilePath file) throws IOException {
        return file.newInputStream();
    }

    private void backupFiles(String toFile, Long lastDate) {
        String path = getStoragePath(); // 可能是一个文件或目录
        if (!toFile.toLowerCase().endsWith(".zip"))
            toFile += ".zip";
        try (OutputStream zip = FileUtils.newOutputStream(toFile, false);
                ZipOutputStream out = new ZipOutputStream(zip)) {
            FilePath p = FilePath.get(path);
            if (p.isDirectory()) {
                String pathShortName = path.replace('\\', '/');
                pathShortName = pathShortName.substring(pathShortName.lastIndexOf('/') + 1);
                FilePath dir = FilePath.get(path);
                for (FilePath map : dir.newDirectoryStream()) {
                    String mapName = map.getName();
                    String entryNameBase = pathShortName + "/" + mapName;
                    for (FilePath file : map.newDirectoryStream()) {
                        if (lastDate == null || file.lastModified() > lastDate.longValue())
                            backupFile(out, getInputStream(mapName, file),
                                    entryNameBase + "/" + file.getName());
                    }
                }
            } else {
                backupFile(out, p.newInputStream(), p.getName());
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, toFile);
        }
    }

    private static void backupFile(ZipOutputStream out, InputStream in, String entryName)
            throws IOException {
        if (in == null)
            return;
        out.putNextEntry(new ZipEntry(entryName));
        IOUtils.copyAndCloseInput(in, out);
        out.closeEntry();
    }
}
