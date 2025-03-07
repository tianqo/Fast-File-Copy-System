import os
import shutil
import tarfile
import threading
from queue import Queue
import sqlite3
import win32file
import time

class FastCopySystem:
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
        self.large_file_threshold = 1024 * 1024 * 100  # 100MB
        self.index_db = "file_index.db"
        self._init_index_db()
        
    def _init_index_db(self):
        """初始化索引数据库"""
        self.conn = sqlite3.connect(self.index_db)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS file_index
                             (path TEXT PRIMARY KEY, size INTEGER, mtime REAL)''')

    def _update_index(self, path):
        """更新文件索引"""
        stat = os.stat(path)
        self.cursor.execute("REPLACE INTO file_index VALUES (?, ?, ?)",
                           (path, stat.st_size, stat.st_mtime))
        self.conn.commit()

    def _get_ntfs_changes(self):
        """Windows NTFS变更监控（简化实现）"""
        # 此处需要实现USN日志监控，此处简化为全量扫描
        for root, dirs, files in os.walk(self.src):
            for f in files:
                path = os.path.join(root, f)
                self._update_index(path)

    def _archive_small_files(self):
        """归档小文件"""
        small_files = []
        for root, dirs, files in os.walk(self.src):
            for f in files:
                path = os.path.join(root, f)
                if os.path.getsize(path) < self.large_file_threshold:
                    small_files.append(path)
        
        # 创建内存中的tar归档
        with tarfile.open(os.path.join(self.dest, "_temp_archive.tar"), "w") as tar:
            for file in small_files:
                tar.add(file)
        return len(small_files)

    def _copy_large_file(self, src, dest):
        """多线程大文件复制"""
        file_size = os.path.getsize(src)
        threads = 4
        chunk_size = file_size // threads
        
        with open(src, 'rb') as fsrc, open(dest, 'wb') as fdest:
            threads = []
            for i in range(threads):
                offset = i * chunk_size
                size = chunk_size if i != threads-1 else file_size - offset
                
                t = threading.Thread(
                    target=self._copy_chunk,
                    args=(fsrc, fdest, offset, size)
                )
                threads.append(t)
                t.start()
            
            for t in threads:
                t.join()

    def _copy_chunk(self, fsrc, fdest, offset, size):
        """文件分块复制"""
        fsrc.seek(offset)
        while size > 0:
            buf = fsrc.read(min(1024*1024, size))  # 1MB buffer
            fdest.write(buf)
            size -= len(buf)

    def run(self):
        """执行拷贝流程"""
        start_time = time.time()
        
        # 阶段1：索引更新
        print("[*] 更新文件索引...")
        self._get_ntfs_changes()
        
        # 阶段2：归档小文件
        print("[*] 归档小文件...")
        archived_count = self._archive_small_files()
        
        # 阶段3：多线程复制大文件
        print("[*] 复制大文件...")
        large_files = [f for f in self.cursor.execute(
            "SELECT path FROM file_index WHERE size >= ?", 
            (self.large_file_threshold,))]
        
        for path, in large_files:
            rel_path = os.path.relpath(path, self.src)
            dest_path = os.path.join(self.dest, rel_path)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            self._copy_large_file(path, dest_path)
        
        # 阶段4：解压归档文件
        print("[*] 解压小文件归档...")
        with tarfile.open(os.path.join(self.dest, "_temp_archive.tar"), "r") as tar:
            tar.extractall(self.dest)
        
        os.remove(os.path.join(self.dest, "_temp_archive.tar"))
        
        print(f"[+] 完成！耗时：{time.time()-start_time:.2f}秒")
        print(f"    归档小文件数量：{archived_count}")
        print(f"    复制大文件数量：{len(large_files)}")

if __name__ == "__main__":
    # 示例用法
    copier = FastCopySystem(
        src=r"D:\Minecraft",
        dest=r"E:\Backup"
    )
    copier.run()