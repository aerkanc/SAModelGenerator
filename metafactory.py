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
            models += "%sTable = Table(u'%s', Base.metadata,\n %s%s)\n\n" % (str(t[1]).title(), t[1], metafactory.colums(cur, t[1]), metafactory.fk(cur, t[1], schema))
            if classes != "":
                classes +="\n\n"
            classes += "class %s(Base):\n    __table__ = %sTable%s" % ( str(t[1]).title(), str(t[1]).title(), metafactory.br(cur, t[1]))

        return models+"\n\n"+classes
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
            elif re.search("timestamp", dt):
                dt = "TIMESTAMP()"
            else:
                dt = dt.upper()+"()"
            if cols != "":
                cols += ",\n"
            cols += "    Column(u'%s', %s" % (c[1], dt)
            if c[6]:
                if re.search("nextval\('", c[7]):
                    cols += ", Sequence('%s')" % str(c[7]).replace("nextval('", "").replace("'::regclass)", "")
                else:
                    cols += ", server_default= text('%s')" % c[7]
            if c[8] == "p":
                cols += ", primary_key=True"
            if c[5]:
                cols += ", nullable=False"
            cols += ")"
        cols = "   #column definitions\n"+ cols
        return cols

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
            foreignkeys += "    %s = relationship('%s', backref='%ss')" % (str(f[2]).title(), str(f[2]).title(), str(tablename).title())

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

