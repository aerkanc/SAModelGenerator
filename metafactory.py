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
            if classes != "":
                classes +="\n\n"
            classes += "class %s(Base):\n    __tablename__ = '%s'\n%s%s%s" % (str(t[1]).title(), t[1], metafactory.colums(cur, t[1], schema), metafactory.rs(cur, t[1]), metafactory.br(cur, t[1]))

        return models+"\n\n"+classes
    @staticmethod
    def colums(cur, tablename, schema ="public"):
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
            cols += "    %s = Column(%s" % (c[1], dt)
            if c[6]:
                if re.search("nextval\('", c[7]):
                    cols += ", Sequence('%s')" % str(c[7]).replace("nextval('", "").replace("'::regclass)", "")
                else:
                    cols += ", server_default= text('%s')" % c[7]
            if c[8] == "p":
                cols += ", primary_key=True"
            elif c[8] == "f":
                cols += ", ForeignKey('%s')" % (metafactory.fk(cur,tablename,c[1], schema))
            if c[5]:
                cols += ", nullable=False"

            cols += ")"
        cols = "    #column definitions\n"+ cols
        return cols

    @staticmethod
    def fk(cur, tablename,column,schema="public"):
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
            AND pga2.attname = '%s'
        """ % (tablename, column))
        fks = cur.fetchall()
        return "%s.%s.%s" % (schema, fks[0][2], fks[0][3])
    @staticmethod
    def rs(cur, tablename):
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
        fks = cur.fetchall()
        foreignkeys =""

        for f in fks:
            if foreignkeys != "":
                foreignkeys += "\n"
            foreignkeys += "    %s = relationship(%s, primaryjoin=%s == %s.%s)" % (f[2], str(f[2]).title(), f[1], str(f[2]).title(), f[3])

        if foreignkeys != "":
            foreignkeys = "\n\n    #relation definitions: many to one with backref\n"+foreignkeys

        return foreignkeys


    def br(cur, tablename):
        cur.execute("""
            SELECT pg_constraint.conname  as fkname, pga1.attname as colname, pc1.relname as referenced_table_name, pga2.attname as referenced_column_name
            FROM pg_class pc1, pg_class pc2, pg_constraint, pg_attribute pga1, pg_attribute pga2
            WHERE pg_constraint.conrelid = pc1.oid
            AND pc2.oid = pg_constraint.confrelid
            AND pga1.attnum = pg_constraint.confkey[1]
            AND pga1.attrelid = pc2.oid
            AND pga2.attnum = pg_constraint.conkey[1]
            AND pga2.attrelid = pc1.oid
            AND pc2.relname = '%s'
        """ % tablename)
        fks = cur.fetchall()
        foreignkeys =""

        for f in fks:
            if foreignkeys != "":
                foreignkeys += "\n"
            foreignkeys += "    %ss = relationship('%s', backref='%s')" % (f[2], str(f[2]).title(), tablename)

        if foreignkeys != "":
            foreignkeys = "\n\n    #relation definitions: one to many with backref\n"+foreignkeys

        return foreignkeys

