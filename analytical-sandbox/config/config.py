class Base():
    debug = False
    testing = False
    s3_path = ''
    stage_path = s3_path+''
    catalog_path = ''
    schema_path = ''
    csv_path = ''
    schema_name = ''

class Acceptance(Base):
    debug = True
    development = True

class Staging(Base):
    debug = False
    testing = True

class Production(Base):
    debug = False
    testing = False