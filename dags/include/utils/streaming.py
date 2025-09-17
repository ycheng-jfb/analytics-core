class RowsToFileLike:
    def __init__(self, rows):
        self.rows = rows
        self.buf = None

    def reload_buf(self, size):
        try:
            cur_len = len(self.buf)
        except TypeError:
            cur_len = 0
        while cur_len < size:
            try:
                next_ = next(self.rows)
                cur_len += len(next_)
                if not self.buf:
                    self.buf = next_
                else:
                    self.buf += next_
            except StopIteration:
                break

    def read(self, size):
        self.reload_buf(size)
        ret = self.buf[:size]
        self.buf = self.buf[size:]
        return ret
