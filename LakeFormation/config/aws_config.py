class Base():
    debug = False
    testing = False
    s3_bucket = ''
    region = 'us-east-1'
    database = 'default'
    queryResults = ''
    query = "SELECT distinct table_schema, table_name FROM information_schema.tables where table_schema not in ('information_schema')"
    workGroup = ''

class Acceptance(Base):
    debug = True
    development = True

class Staging(Base):
    debug = False
    testing = True

class Production(Base):
    debug = False
    testing = False