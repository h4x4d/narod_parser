FILENAME = 'Narod.txt'

DATABASE = 'sites.db'

BINARY_FILES = {'png', 'jpg', 'pdf', 'css', 'js', 'zip', 'rar', 'docx', 'doc',
                'pptx', 'xlsx', 'wmv', 'mp4', 'mp3', 'wma', 'jpeg', 'gif'}
BINARY_FILES |= {i.upper() for i in BINARY_FILES}

LIMIT = 10
