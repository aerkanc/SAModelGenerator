from sqlalchemy.sql.elements import Null

__author__ = 'Ahmet Erkan ÇELİK'
import re


class metafactory:
    @staticmethod
    def tables(cur, schema="public"):
        cur.execute("""SELECT * FROM  pg_tables WHERE schemaname = '%s'""" % schema)
        ts = cur.fetchall()
        models = ""
        classes = ""
        for t in ts:
            # models += "%sTable = Table(u'%s', Base.metadata,\n %s%s,\n\n    #schema\n    schema='%s'\n)\n\n" % (str(t[1]).title(), t[1], metafactory.colums(cur, t[1]), metafactory.fk(cur, t[1], schema),schema)
            if classes != "":
                classes +="\n\n\n"
            tableName = str(t[1])
            className = tableName.title().replace('_', '')
            colums = metafactory.colums(cur, t[1])
            br = metafactory.br(cur, t[1])

            classes += "class %s(Base):\n    __tablename__ = u'%s'%s%s\n\n%s" % (className, tableName, colums, br, metafactory.toJsonMethod(cur, t[1]))

        return classes
    @staticmethod
    def colums(cur, tablename):
        cur.execute("""
                        SELECT DISTINCT ON (attnum) pg_attribute.attnum,pg_attribute.attname as column_name,
                           format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as data_type,
                           pg_attribute.attlen as lenght, pg_attribute.atttypmod as lenght_var,
                           pg_attribute.attnotnull as is_notnull,
                           pg_attribute.atthasdef as has_default,
                           adsrc as default_value,
                           pg_constraint.contype
                        FROM
                          pg_attribute
                          INNER JOIN pg_class ON (pg_attribute.attrelid = pg_class.oid)
                          INNER JOIN pg_type ON (pg_attribute.atttypid = pg_type.oid)
                          LEFT OUTER JOIN pg_attrdef ON (pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum=pg_attrdef.adnum)
                          LEFT OUTER JOIN pg_index ON (pg_class.oid = pg_index.indrelid AND pg_attribute.attnum = any(pg_index.indkey))
                          LEFT OUTER JOIN pg_constraint ON (pg_constraint.conrelid = pg_class.oid AND pg_constraint.conkey[1]= pg_attribute.attnum)
                         WHERE pg_class.relname = '%s'
                         AND pg_attribute.attnum>0
                    """ % tablename)
        cs = cur.fetchall()
        cols = ""
        for c in cs:
            dt = c[2]
            if re.search("character varying", dt):
                dt = "VARCHAR(length=%s)" % (int(c[4])-4)
            elif re.search("character", dt):
                dt = "CHAR(length=%s)" % (int(c[4])-4)
            elif re.search("timestamp", dt):
                dt = "TIMESTAMP()"
            else:
                dt = dt.replace(" ","_").upper()+"()"
            if cols != "":
                cols += "\n"
            cols += "    %s = Column(%s" % (c[1], dt)
            if c[6]:
                if re.search("nextval\('", c[7]):
                    cols += ", Sequence('%s')" % str(c[7]).replace("nextval('", "").replace("'::regclass)", "")
                else:
                    cols += ", server_default=text('%s')" % c[7]
            if c[8] == "p":
                cols += ", primary_key=True"
            elif c[8] == "f":
                cols += ", ForeignKey('%s')" % metafactory.isFk(cur,tablename,c[1])
            if c[5]:
                cols += ", nullable=False"
            cols += ")"
        cols = "\n\n    # column definitions\n"+ cols
        return cols

    @staticmethod
    def isFk(cur, tablename, column):
        fks = metafactory.forein_keys(cur, tablename)
        for c in fks:
            if c[1] == column:
                return "%s.%s" % (c[2], c[3])
        else:
            return Null

    @staticmethod
    def fk(cur, tablename,schema="public"):
        fks = metafactory.forein_keys(cur, tablename)
        foreignkeys =""
        for f in fks:
            if foreignkeys != "":
                foreignkeys += ",\n"
            foreignkeys += "    ForeignKeyConstraint(['%s'],['%s.%s.%s'],name='%s')" % (f[1], schema, f[2], f[3], f[0])

        if foreignkeys!="":
            foreignkeys = ",\n\n    #foreign keys\n"+foreignkeys

        return foreignkeys

    def br(cur, tablename):
        fks = metafactory.forein_keys(cur, tablename)
        foreignkeys =""
        for f in fks:
            if foreignkeys != "":
                foreignkeys += "\n"
            col = str(f[1])
            parentTable=str(f[2]).title().replace("_","")
            tableClass = str(tablename).title().replace("_", "")
            var = tableClass + ''.join(col.rsplit('_id', 1)).title().replace('_', '')
            parentCol = f[3]
            foreignkeys += "    %s = relationship('%s', primaryjoin='%s.%s == %s.%s')" % (var, parentTable, tableClass ,col, parentTable, parentCol)
            # foreignkeys += "    %s = relationship('%s')" % (var, parentTable)

        if foreignkeys!="":
            foreignkeys = "\n\n    #relation definitions: many to one with backref (also takes care of one to many)\n"+foreignkeys

        return foreignkeys

    @staticmethod
    def forein_keys(cur, tablename, many_to_one = False):
        cur.execute("""
            SELECT pg_constraint.conname  as fkname, pga2.attname as colname, pc2.relname as referenced_table_name, pga1.attname as referenced_column_name
            FROM pg_class pc1, pg_class pc2, pg_constraint, pg_attribute pga1, pg_attribute pga2
            WHERE pg_constraint.conrelid = pc1.oid
            AND pc2.oid = pg_constraint.confrelid
            AND pga1.attnum = pg_constraint.confkey[1]
            AND pga1.attrelid = pc2.oid
            AND pga2.attnum = pg_constraint.conkey[1]
            AND pga2.attrelid = pc1.oid
            AND pc1.relname = '%s'
        """ % tablename)
        return cur.fetchall()

    @staticmethod
    def toJsonMethod(cur, tablename):
        metod = "    def to_json(self):\n        obj = {"
        cur.execute("""
                        SELECT DISTINCT ON (attnum) pg_attribute.attnum,pg_attribute.attname as column_name,
                           format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as data_type,
                           pg_attribute.attlen as lenght, pg_attribute.atttypmod as lenght_var,
                           pg_attribute.attnotnull as is_notnull,
                           pg_attribute.atthasdef as has_default,
                           adsrc as default_value,
                           pg_constraint.contype
                        FROM
                          pg_attribute
                          INNER JOIN pg_class ON (pg_attribute.attrelid = pg_class.oid)
                          INNER JOIN pg_type ON (pg_attribute.atttypid = pg_type.oid)
                          LEFT OUTER JOIN pg_attrdef ON (pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum=pg_attrdef.adnum)
                          LEFT OUTER JOIN pg_index ON (pg_class.oid = pg_index.indrelid AND pg_attribute.attnum = any(pg_index.indkey))
                          LEFT OUTER JOIN pg_constraint ON (pg_constraint.conrelid = pg_class.oid AND pg_constraint.conkey[1]= pg_attribute.attnum)
                         WHERE pg_class.relname = '%s'
                         AND pg_attribute.attnum>0
                    """ % tablename)
        cs = cur.fetchall()
        for c in cs:
            if re.search("timestamp",c[2]):
                metod += ("\n            '%s': self.%s" % (c[1], c[1])) + ".strftime('%a, %d %b %Y %H:%M:%S +0000') if " + ("self.%s else None," % (c[1]))
            else:
                metod += "\n            '%s': self.%s," % (c[1], c[1])
        metod += "\n        }\n        return json.dumps(obj)"
        return metod
