class SqlGlotException(Exception):
    def __init__(self, message, filename, table):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        self.message = message
        self.filename = filename
        self.table = table

    def __str__(self):
        return '%s (Table=%s, Filename=%s)' % (self.message, self.table, self.filename)


class SqlLeafException(Exception):
    def __init__(self, message, filename, table):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        self.message = message
        self.filename = filename
        self.table = table

    def __str__(self):
        return '%s (Table=%s, Filename=%s)' % (self.message, self.table, self.filename)
