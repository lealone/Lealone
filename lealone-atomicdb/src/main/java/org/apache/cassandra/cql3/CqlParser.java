// $ANTLR 3.2 Sep 23, 2009 12:02:23 E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g 2013-12-18 11:29:33

    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.auth.DataResource;
    import org.apache.cassandra.auth.IResource;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.functions.FunctionCall;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "K_USE", "K_SELECT", "K_DISTINCT", "K_COUNT", "K_AS", "K_FROM", "K_WHERE", "K_ORDER", "K_BY", "K_LIMIT", "K_ALLOW", "K_FILTERING", "K_WRITETIME", "K_TTL", "INTEGER", "K_AND", "K_ASC", "K_DESC", "K_INSERT", "K_INTO", "K_VALUES", "K_IF", "K_NOT", "K_EXISTS", "K_USING", "K_TIMESTAMP", "K_UPDATE", "K_SET", "K_DELETE", "K_BEGIN", "K_UNLOGGED", "K_COUNTER", "K_BATCH", "K_APPLY", "K_CREATE", "K_KEYSPACE", "K_WITH", "K_COLUMNFAMILY", "K_PRIMARY", "K_KEY", "K_COMPACT", "K_STORAGE", "K_CLUSTERING", "K_TYPE", "K_CUSTOM", "K_INDEX", "IDENT", "K_ON", "STRING_LITERAL", "K_TRIGGER", "K_DROP", "K_ALTER", "K_ADD", "K_RENAME", "K_TO", "K_TRUNCATE", "K_GRANT", "K_REVOKE", "K_LIST", "K_OF", "K_NORECURSIVE", "K_MODIFY", "K_AUTHORIZE", "K_ALL", "K_PERMISSIONS", "K_PERMISSION", "K_KEYSPACES", "K_USER", "K_SUPERUSER", "K_NOSUPERUSER", "K_USERS", "K_PASSWORD", "QUOTED_NAME", "FLOAT", "BOOLEAN", "UUID", "HEXNUMBER", "K_NAN", "K_INFINITY", "K_NULL", "QMARK", "K_TOKEN", "K_IN", "K_CONTAINS", "K_ASCII", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_DECIMAL", "K_DOUBLE", "K_FLOAT", "K_INET", "K_INT", "K_TEXT", "K_UUID", "K_VARCHAR", "K_VARINT", "K_TIMEUUID", "K_MAP", "S", "E", "L", "C", "T", "F", "R", "O", "M", "A", "W", "H", "N", "D", "K", "Y", "I", "U", "P", "G", "B", "X", "V", "Z", "J", "Q", "DIGIT", "LETTER", "HEX", "EXPONENT", "WS", "COMMENT", "MULTILINE_COMMENT", "';'", "'('", "')'", "','", "'\\*'", "'.'", "'['", "']'", "'-'", "'{'", "':'", "'}'", "'='", "'+'", "'<'", "'<='", "'>'", "'>='"
    };
    public static final int EXPONENT=132;
    public static final int K_PERMISSIONS=68;
    public static final int LETTER=130;
    public static final int K_INT=96;
    public static final int K_PERMISSION=69;
    public static final int K_CREATE=38;
    public static final int K_CLUSTERING=46;
    public static final int K_WRITETIME=16;
    public static final int K_INFINITY=82;
    public static final int K_EXISTS=27;
    public static final int EOF=-1;
    public static final int K_PRIMARY=42;
    public static final int K_AUTHORIZE=66;
    public static final int K_VALUES=24;
    public static final int K_USE=4;
    public static final int K_DISTINCT=6;
    public static final int T__148=148;
    public static final int STRING_LITERAL=52;
    public static final int T__147=147;
    public static final int K_GRANT=60;
    public static final int T__149=149;
    public static final int K_ON=51;
    public static final int K_USING=28;
    public static final int K_ADD=56;
    public static final int K_ASC=20;
    public static final int K_CUSTOM=48;
    public static final int K_KEY=43;
    public static final int COMMENT=134;
    public static final int K_TRUNCATE=59;
    public static final int T__150=150;
    public static final int K_ORDER=11;
    public static final int T__151=151;
    public static final int HEXNUMBER=80;
    public static final int T__152=152;
    public static final int K_OF=63;
    public static final int K_ALL=67;
    public static final int T__153=153;
    public static final int D=116;
    public static final int T__139=139;
    public static final int E=104;
    public static final int T__138=138;
    public static final int F=108;
    public static final int T__137=137;
    public static final int G=122;
    public static final int T__136=136;
    public static final int K_COUNT=7;
    public static final int K_KEYSPACE=39;
    public static final int K_TYPE=47;
    public static final int A=112;
    public static final int B=123;
    public static final int C=106;
    public static final int L=105;
    public static final int M=111;
    public static final int N=115;
    public static final int O=110;
    public static final int H=114;
    public static final int I=119;
    public static final int J=127;
    public static final int K_UPDATE=30;
    public static final int K=117;
    public static final int K_FILTERING=15;
    public static final int U=120;
    public static final int T=107;
    public static final int W=113;
    public static final int K_TEXT=97;
    public static final int V=125;
    public static final int Q=128;
    public static final int P=121;
    public static final int K_COMPACT=44;
    public static final int S=103;
    public static final int R=109;
    public static final int T__141=141;
    public static final int T__142=142;
    public static final int K_TTL=17;
    public static final int T__140=140;
    public static final int Y=118;
    public static final int T__145=145;
    public static final int X=124;
    public static final int T__146=146;
    public static final int T__143=143;
    public static final int Z=126;
    public static final int T__144=144;
    public static final int K_INDEX=49;
    public static final int K_INSERT=22;
    public static final int WS=133;
    public static final int K_NOT=26;
    public static final int K_RENAME=57;
    public static final int K_APPLY=37;
    public static final int K_INET=95;
    public static final int K_STORAGE=45;
    public static final int K_TIMESTAMP=29;
    public static final int K_NULL=83;
    public static final int K_AND=19;
    public static final int K_DESC=21;
    public static final int K_TOKEN=85;
    public static final int QMARK=84;
    public static final int K_UUID=98;
    public static final int K_BATCH=36;
    public static final int K_ASCII=88;
    public static final int UUID=79;
    public static final int K_LIST=62;
    public static final int K_DELETE=32;
    public static final int K_TO=58;
    public static final int K_BY=12;
    public static final int FLOAT=77;
    public static final int K_VARINT=100;
    public static final int K_FLOAT=94;
    public static final int K_SUPERUSER=72;
    public static final int K_DOUBLE=93;
    public static final int K_SELECT=5;
    public static final int K_LIMIT=13;
    public static final int K_BOOLEAN=91;
    public static final int K_ALTER=55;
    public static final int K_SET=31;
    public static final int K_TRIGGER=53;
    public static final int K_WHERE=10;
    public static final int QUOTED_NAME=76;
    public static final int MULTILINE_COMMENT=135;
    public static final int K_BLOB=90;
    public static final int BOOLEAN=78;
    public static final int K_UNLOGGED=34;
    public static final int HEX=131;
    public static final int K_INTO=23;
    public static final int K_PASSWORD=75;
    public static final int K_REVOKE=61;
    public static final int K_ALLOW=14;
    public static final int K_VARCHAR=99;
    public static final int IDENT=50;
    public static final int DIGIT=129;
    public static final int K_USERS=74;
    public static final int K_BEGIN=33;
    public static final int INTEGER=18;
    public static final int K_KEYSPACES=70;
    public static final int K_COUNTER=35;
    public static final int K_DECIMAL=92;
    public static final int K_CONTAINS=87;
    public static final int K_WITH=40;
    public static final int K_IN=86;
    public static final int K_NORECURSIVE=64;
    public static final int K_MAP=102;
    public static final int K_NAN=81;
    public static final int K_IF=25;
    public static final int K_FROM=9;
    public static final int K_COLUMNFAMILY=41;
    public static final int K_MODIFY=65;
    public static final int K_DROP=54;
    public static final int K_NOSUPERUSER=73;
    public static final int K_AS=8;
    public static final int K_BIGINT=89;
    public static final int K_TIMEUUID=101;
    public static final int K_USER=71;

    // delegates
    // delegators


        public CqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return CqlParser.tokenNames; }
    public String getGrammarFileName() { return "E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g"; }


        private final List<String> recognitionErrors = new ArrayList<String>();
        private final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

        public static final Set<String> reservedTypeNames = new HashSet<String>()
        {{
            add("byte");
            add("smallint");
            add("complex");
            add("enum");
            add("date");
            add("interval");
            add("macaddr");
            add("bitstring");
        }};

        public AbstractMarker.Raw newBindVariables(ColumnIdentifier name)
        {
            AbstractMarker.Raw marker = new AbstractMarker.Raw(bindVariables.size());
            bindVariables.add(name);
            return marker;
        }

        public AbstractMarker.INRaw newINBindVariables(ColumnIdentifier name)
        {
            AbstractMarker.INRaw marker = new AbstractMarker.INRaw(bindVariables.size());
            bindVariables.add(name);
            return marker;
        }

        public void displayRecognitionError(String[] tokenNames, RecognitionException e)
        {
            String hdr = getErrorHeader(e);
            String msg = getErrorMessage(e, tokenNames);
            recognitionErrors.add(hdr + " " + msg);
        }

        public void addRecognitionError(String msg)
        {
            recognitionErrors.add(msg);
        }

        public List<String> getRecognitionErrors()
        {
            return recognitionErrors;
        }

        public void throwLastRecognitionError() throws SyntaxException
        {
            if (recognitionErrors.size() > 0)
                throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
        }

        public Map<String, String> convertPropertyMap(Maps.Literal map)
        {
            if (map == null || map.entries == null || map.entries.isEmpty())
                return Collections.<String, String>emptyMap();

            Map<String, String> res = new HashMap<String, String>(map.entries.size());

            for (Pair<Term.Raw, Term.Raw> entry : map.entries)
            {
                // Because the parser tries to be smart and recover on error (to
                // allow displaying more than one error I suppose), we have null
                // entries in there. Just skip those, a proper error will be thrown in the end.
                if (entry.left == null || entry.right == null)
                    break;

                if (!(entry.left instanceof Constants.Literal))
                {
                    String msg = "Invalid property name: " + entry.left;
                    if (entry.left instanceof AbstractMarker.Raw)
                        msg += " (bind variables are not supported in DDL queries)";
                    addRecognitionError(msg);
                    break;
                }
                if (!(entry.right instanceof Constants.Literal))
                {
                    String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
                    if (entry.right instanceof AbstractMarker.Raw)
                        msg += " (bind variables are not supported in DDL queries)";
                    addRecognitionError(msg);
                    break;
                }

                res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText());
            }

            return res;
        }

        public void addRawUpdate(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, Operation.RawUpdate update)
        {
            for (Pair<ColumnIdentifier, Operation.RawUpdate> p : operations)
            {
                if (p.left.equals(key) && !p.right.isCompatibleWith(update))
                    addRecognitionError("Multiple incompatible setting of column " + key);
            }
            operations.add(Pair.create(key, update));
        }



    // $ANTLR start "query"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:199:1: query returns [ParsedStatement stmnt] : st= cqlStatement ( ';' )* EOF ;
    public final ParsedStatement query() throws RecognitionException {
        ParsedStatement stmnt = null;

        ParsedStatement st = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:202:5: (st= cqlStatement ( ';' )* EOF )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:202:7: st= cqlStatement ( ';' )* EOF
            {
            pushFollow(FOLLOW_cqlStatement_in_query72);
            st=cqlStatement();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:202:23: ( ';' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==136) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:202:24: ';'
            	    {
            	    match(input,136,FOLLOW_136_in_query75); 

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            match(input,EOF,FOLLOW_EOF_in_query79); 
             stmnt = st; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmnt;
    }
    // $ANTLR end "query"


    // $ANTLR start "cqlStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:205:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createTableStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropTableStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement | st23= createTriggerStatement | st24= dropTriggerStatement | st25= createTypeStatement | st26= alterTypeStatement | st27= dropTypeStatement );
    public final ParsedStatement cqlStatement() throws RecognitionException {
        ParsedStatement stmt = null;

        SelectStatement.RawStatement st1 = null;

        UpdateStatement.ParsedInsert st2 = null;

        UpdateStatement.ParsedUpdate st3 = null;

        BatchStatement.Parsed st4 = null;

        DeleteStatement.Parsed st5 = null;

        UseStatement st6 = null;

        TruncateStatement st7 = null;

        CreateKeyspaceStatement st8 = null;

        CreateTableStatement.RawStatement st9 = null;

        CreateIndexStatement st10 = null;

        DropKeyspaceStatement st11 = null;

        DropTableStatement st12 = null;

        DropIndexStatement st13 = null;

        AlterTableStatement st14 = null;

        AlterKeyspaceStatement st15 = null;

        GrantStatement st16 = null;

        RevokeStatement st17 = null;

        ListPermissionsStatement st18 = null;

        CreateUserStatement st19 = null;

        AlterUserStatement st20 = null;

        DropUserStatement st21 = null;

        ListUsersStatement st22 = null;

        CreateTriggerStatement st23 = null;

        DropTriggerStatement st24 = null;

        CreateTypeStatement st25 = null;

        AlterTypeStatement st26 = null;

        DropTypeStatement st27 = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:207:5: (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createTableStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropTableStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement | st23= createTriggerStatement | st24= dropTriggerStatement | st25= createTypeStatement | st26= alterTypeStatement | st27= dropTypeStatement )
            int alt2=27;
            alt2 = dfa2.predict(input);
            switch (alt2) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:207:7: st1= selectStatement
                    {
                    pushFollow(FOLLOW_selectStatement_in_cqlStatement113);
                    st1=selectStatement();

                    state._fsp--;

                     stmt = st1; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:208:7: st2= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_cqlStatement138);
                    st2=insertStatement();

                    state._fsp--;

                     stmt = st2; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:209:7: st3= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_cqlStatement163);
                    st3=updateStatement();

                    state._fsp--;

                     stmt = st3; 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:210:7: st4= batchStatement
                    {
                    pushFollow(FOLLOW_batchStatement_in_cqlStatement188);
                    st4=batchStatement();

                    state._fsp--;

                     stmt = st4; 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:211:7: st5= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_cqlStatement214);
                    st5=deleteStatement();

                    state._fsp--;

                     stmt = st5; 

                    }
                    break;
                case 6 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:212:7: st6= useStatement
                    {
                    pushFollow(FOLLOW_useStatement_in_cqlStatement239);
                    st6=useStatement();

                    state._fsp--;

                     stmt = st6; 

                    }
                    break;
                case 7 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:213:7: st7= truncateStatement
                    {
                    pushFollow(FOLLOW_truncateStatement_in_cqlStatement267);
                    st7=truncateStatement();

                    state._fsp--;

                     stmt = st7; 

                    }
                    break;
                case 8 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:214:7: st8= createKeyspaceStatement
                    {
                    pushFollow(FOLLOW_createKeyspaceStatement_in_cqlStatement290);
                    st8=createKeyspaceStatement();

                    state._fsp--;

                     stmt = st8; 

                    }
                    break;
                case 9 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:215:7: st9= createTableStatement
                    {
                    pushFollow(FOLLOW_createTableStatement_in_cqlStatement307);
                    st9=createTableStatement();

                    state._fsp--;

                     stmt = st9; 

                    }
                    break;
                case 10 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:216:7: st10= createIndexStatement
                    {
                    pushFollow(FOLLOW_createIndexStatement_in_cqlStatement326);
                    st10=createIndexStatement();

                    state._fsp--;

                     stmt = st10; 

                    }
                    break;
                case 11 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:217:7: st11= dropKeyspaceStatement
                    {
                    pushFollow(FOLLOW_dropKeyspaceStatement_in_cqlStatement345);
                    st11=dropKeyspaceStatement();

                    state._fsp--;

                     stmt = st11; 

                    }
                    break;
                case 12 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:218:7: st12= dropTableStatement
                    {
                    pushFollow(FOLLOW_dropTableStatement_in_cqlStatement363);
                    st12=dropTableStatement();

                    state._fsp--;

                     stmt = st12; 

                    }
                    break;
                case 13 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:219:7: st13= dropIndexStatement
                    {
                    pushFollow(FOLLOW_dropIndexStatement_in_cqlStatement384);
                    st13=dropIndexStatement();

                    state._fsp--;

                     stmt = st13; 

                    }
                    break;
                case 14 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:220:7: st14= alterTableStatement
                    {
                    pushFollow(FOLLOW_alterTableStatement_in_cqlStatement405);
                    st14=alterTableStatement();

                    state._fsp--;

                     stmt = st14; 

                    }
                    break;
                case 15 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:221:7: st15= alterKeyspaceStatement
                    {
                    pushFollow(FOLLOW_alterKeyspaceStatement_in_cqlStatement425);
                    st15=alterKeyspaceStatement();

                    state._fsp--;

                     stmt = st15; 

                    }
                    break;
                case 16 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:222:7: st16= grantStatement
                    {
                    pushFollow(FOLLOW_grantStatement_in_cqlStatement442);
                    st16=grantStatement();

                    state._fsp--;

                     stmt = st16; 

                    }
                    break;
                case 17 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:223:7: st17= revokeStatement
                    {
                    pushFollow(FOLLOW_revokeStatement_in_cqlStatement467);
                    st17=revokeStatement();

                    state._fsp--;

                     stmt = st17; 

                    }
                    break;
                case 18 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:224:7: st18= listPermissionsStatement
                    {
                    pushFollow(FOLLOW_listPermissionsStatement_in_cqlStatement491);
                    st18=listPermissionsStatement();

                    state._fsp--;

                     stmt = st18; 

                    }
                    break;
                case 19 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:225:7: st19= createUserStatement
                    {
                    pushFollow(FOLLOW_createUserStatement_in_cqlStatement506);
                    st19=createUserStatement();

                    state._fsp--;

                     stmt = st19; 

                    }
                    break;
                case 20 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:226:7: st20= alterUserStatement
                    {
                    pushFollow(FOLLOW_alterUserStatement_in_cqlStatement526);
                    st20=alterUserStatement();

                    state._fsp--;

                     stmt = st20; 

                    }
                    break;
                case 21 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:227:7: st21= dropUserStatement
                    {
                    pushFollow(FOLLOW_dropUserStatement_in_cqlStatement547);
                    st21=dropUserStatement();

                    state._fsp--;

                     stmt = st21; 

                    }
                    break;
                case 22 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:228:7: st22= listUsersStatement
                    {
                    pushFollow(FOLLOW_listUsersStatement_in_cqlStatement569);
                    st22=listUsersStatement();

                    state._fsp--;

                     stmt = st22; 

                    }
                    break;
                case 23 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:229:7: st23= createTriggerStatement
                    {
                    pushFollow(FOLLOW_createTriggerStatement_in_cqlStatement590);
                    st23=createTriggerStatement();

                    state._fsp--;

                     stmt = st23; 

                    }
                    break;
                case 24 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:230:7: st24= dropTriggerStatement
                    {
                    pushFollow(FOLLOW_dropTriggerStatement_in_cqlStatement607);
                    st24=dropTriggerStatement();

                    state._fsp--;

                     stmt = st24; 

                    }
                    break;
                case 25 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:231:7: st25= createTypeStatement
                    {
                    pushFollow(FOLLOW_createTypeStatement_in_cqlStatement626);
                    st25=createTypeStatement();

                    state._fsp--;

                     stmt = st25; 

                    }
                    break;
                case 26 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:232:7: st26= alterTypeStatement
                    {
                    pushFollow(FOLLOW_alterTypeStatement_in_cqlStatement646);
                    st26=alterTypeStatement();

                    state._fsp--;

                     stmt = st26; 

                    }
                    break;
                case 27 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:233:7: st27= dropTypeStatement
                    {
                    pushFollow(FOLLOW_dropTypeStatement_in_cqlStatement667);
                    st27=dropTypeStatement();

                    state._fsp--;

                     stmt = st27; 

                    }
                    break;

            }
             if (stmt != null) stmt.setBoundVariables(bindVariables); 
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "cqlStatement"


    // $ANTLR start "useStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:239:1: useStatement returns [UseStatement stmt] : K_USE ks= keyspaceName ;
    public final UseStatement useStatement() throws RecognitionException {
        UseStatement stmt = null;

        String ks = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:240:5: ( K_USE ks= keyspaceName )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:240:7: K_USE ks= keyspaceName
            {
            match(input,K_USE,FOLLOW_K_USE_in_useStatement702); 
            pushFollow(FOLLOW_keyspaceName_in_useStatement706);
            ks=keyspaceName();

            state._fsp--;

             stmt = new UseStatement(ks); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "useStatement"


    // $ANTLR start "selectStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:243:1: selectStatement returns [SelectStatement.RawStatement expr] : K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )? ;
    public final SelectStatement.RawStatement selectStatement() throws RecognitionException {
        SelectStatement.RawStatement expr = null;

        List<RawSelector> sclause = null;

        ColumnIdentifier c = null;

        CFName cf = null;

        List<Relation> wclause = null;

        Term.Raw rows = null;



                boolean isDistinct = false;
                boolean isCount = false;
                ColumnIdentifier countAlias = null;
                Term.Raw limit = null;
                Map<ColumnIdentifier, Boolean> orderings = new LinkedHashMap<ColumnIdentifier, Boolean>();
                boolean allowFiltering = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:5: ( K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:7: K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )?
            {
            match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement740); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:16: ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==K_DISTINCT||LA5_0==K_AS||(LA5_0>=K_FILTERING && LA5_0<=K_TTL)||LA5_0==K_VALUES||LA5_0==K_EXISTS||LA5_0==K_TIMESTAMP||LA5_0==K_COUNTER||(LA5_0>=K_KEY && LA5_0<=K_CUSTOM)||LA5_0==IDENT||LA5_0==K_TRIGGER||LA5_0==K_LIST||(LA5_0>=K_ALL && LA5_0<=QUOTED_NAME)||LA5_0==K_TOKEN||(LA5_0>=K_CONTAINS && LA5_0<=K_MAP)||LA5_0==140) ) {
                alt5=1;
            }
            else if ( (LA5_0==K_COUNT) ) {
                int LA5_2 = input.LA(2);

                if ( (LA5_2==137) ) {
                    alt5=2;
                }
                else if ( ((LA5_2>=K_AS && LA5_2<=K_FROM)||LA5_2==139||LA5_2==141) ) {
                    alt5=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 2, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:18: ( K_DISTINCT )? sclause= selectClause
                    {
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:18: ( K_DISTINCT )?
                    int alt3=2;
                    int LA3_0 = input.LA(1);

                    if ( (LA3_0==K_DISTINCT) ) {
                        int LA3_1 = input.LA(2);

                        if ( (LA3_1==K_AS) ) {
                            int LA3_3 = input.LA(3);

                            if ( (LA3_3==K_FROM||LA3_3==137||LA3_3==139||LA3_3==141) ) {
                                alt3=1;
                            }
                            else if ( (LA3_3==K_AS) ) {
                                int LA3_5 = input.LA(4);

                                if ( ((LA3_5>=K_DISTINCT && LA3_5<=K_AS)||(LA3_5>=K_FILTERING && LA3_5<=K_TTL)||LA3_5==K_VALUES||LA3_5==K_EXISTS||LA3_5==K_TIMESTAMP||LA3_5==K_COUNTER||(LA3_5>=K_KEY && LA3_5<=K_CUSTOM)||LA3_5==IDENT||LA3_5==K_TRIGGER||LA3_5==K_LIST||(LA3_5>=K_ALL && LA3_5<=QUOTED_NAME)||(LA3_5>=K_CONTAINS && LA3_5<=K_MAP)) ) {
                                    alt3=1;
                                }
                            }
                        }
                        else if ( ((LA3_1>=K_DISTINCT && LA3_1<=K_COUNT)||(LA3_1>=K_FILTERING && LA3_1<=K_TTL)||LA3_1==K_VALUES||LA3_1==K_EXISTS||LA3_1==K_TIMESTAMP||LA3_1==K_COUNTER||(LA3_1>=K_KEY && LA3_1<=K_CUSTOM)||LA3_1==IDENT||LA3_1==K_TRIGGER||LA3_1==K_LIST||(LA3_1>=K_ALL && LA3_1<=QUOTED_NAME)||LA3_1==K_TOKEN||(LA3_1>=K_CONTAINS && LA3_1<=K_MAP)||LA3_1==140) ) {
                            alt3=1;
                        }
                    }
                    switch (alt3) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:258:20: K_DISTINCT
                            {
                            match(input,K_DISTINCT,FOLLOW_K_DISTINCT_in_selectStatement746); 
                             isDistinct = true; 

                            }
                            break;

                    }

                    pushFollow(FOLLOW_selectClause_in_selectStatement755);
                    sclause=selectClause();

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:259:18: ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? )
                    {
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:259:18: ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )? )
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:259:19: K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= cident )?
                    {
                    match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement775); 
                    match(input,137,FOLLOW_137_in_selectStatement777); 
                    pushFollow(FOLLOW_selectCountClause_in_selectStatement781);
                    sclause=selectCountClause();

                    state._fsp--;

                    match(input,138,FOLLOW_138_in_selectStatement783); 
                     isCount = true; 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:259:81: ( K_AS c= cident )?
                    int alt4=2;
                    int LA4_0 = input.LA(1);

                    if ( (LA4_0==K_AS) ) {
                        alt4=1;
                    }
                    switch (alt4) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:259:82: K_AS c= cident
                            {
                            match(input,K_AS,FOLLOW_K_AS_in_selectStatement788); 
                            pushFollow(FOLLOW_cident_in_selectStatement792);
                            c=cident();

                            state._fsp--;

                             countAlias = c; 

                            }
                            break;

                    }


                    }


                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement807); 
            pushFollow(FOLLOW_columnFamilyName_in_selectStatement811);
            cf=columnFamilyName();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:261:7: ( K_WHERE wclause= whereClause )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==K_WHERE) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:261:9: K_WHERE wclause= whereClause
                    {
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement821); 
                    pushFollow(FOLLOW_whereClause_in_selectStatement825);
                    wclause=whereClause();

                    state._fsp--;


                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:262:7: ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==K_ORDER) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:262:9: K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )*
                    {
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_selectStatement838); 
                    match(input,K_BY,FOLLOW_K_BY_in_selectStatement840); 
                    pushFollow(FOLLOW_orderByClause_in_selectStatement842);
                    orderByClause(orderings);

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:262:47: ( ',' orderByClause[orderings] )*
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( (LA7_0==139) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:262:49: ',' orderByClause[orderings]
                    	    {
                    	    match(input,139,FOLLOW_139_in_selectStatement847); 
                    	    pushFollow(FOLLOW_orderByClause_in_selectStatement849);
                    	    orderByClause(orderings);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop7;
                        }
                    } while (true);


                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:263:7: ( K_LIMIT rows= intValue )?
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==K_LIMIT) ) {
                alt9=1;
            }
            switch (alt9) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:263:9: K_LIMIT rows= intValue
                    {
                    match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement866); 
                    pushFollow(FOLLOW_intValue_in_selectStatement870);
                    rows=intValue();

                    state._fsp--;

                     limit = rows; 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:264:7: ( K_ALLOW K_FILTERING )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==K_ALLOW) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:264:9: K_ALLOW K_FILTERING
                    {
                    match(input,K_ALLOW,FOLLOW_K_ALLOW_in_selectStatement885); 
                    match(input,K_FILTERING,FOLLOW_K_FILTERING_in_selectStatement887); 
                     allowFiltering = true; 

                    }
                    break;

            }


                      SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
                                                                                         isDistinct,
                                                                                         isCount,
                                                                                         countAlias,
                                                                                         allowFiltering);
                      expr = new SelectStatement.RawStatement(cf, params, sclause, wclause, limit);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectStatement"


    // $ANTLR start "selectClause"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:275:1: selectClause returns [List<RawSelector> expr] : (t1= selector ( ',' tN= selector )* | '\\*' );
    public final List<RawSelector> selectClause() throws RecognitionException {
        List<RawSelector> expr = null;

        RawSelector t1 = null;

        RawSelector tN = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:276:5: (t1= selector ( ',' tN= selector )* | '\\*' )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( ((LA12_0>=K_DISTINCT && LA12_0<=K_AS)||(LA12_0>=K_FILTERING && LA12_0<=K_TTL)||LA12_0==K_VALUES||LA12_0==K_EXISTS||LA12_0==K_TIMESTAMP||LA12_0==K_COUNTER||(LA12_0>=K_KEY && LA12_0<=K_CUSTOM)||LA12_0==IDENT||LA12_0==K_TRIGGER||LA12_0==K_LIST||(LA12_0>=K_ALL && LA12_0<=QUOTED_NAME)||LA12_0==K_TOKEN||(LA12_0>=K_CONTAINS && LA12_0<=K_MAP)) ) {
                alt12=1;
            }
            else if ( (LA12_0==140) ) {
                alt12=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:276:7: t1= selector ( ',' tN= selector )*
                    {
                    pushFollow(FOLLOW_selector_in_selectClause924);
                    t1=selector();

                    state._fsp--;

                     expr = new ArrayList<RawSelector>(); expr.add(t1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:276:76: ( ',' tN= selector )*
                    loop11:
                    do {
                        int alt11=2;
                        int LA11_0 = input.LA(1);

                        if ( (LA11_0==139) ) {
                            alt11=1;
                        }


                        switch (alt11) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:276:77: ',' tN= selector
                    	    {
                    	    match(input,139,FOLLOW_139_in_selectClause929); 
                    	    pushFollow(FOLLOW_selector_in_selectClause933);
                    	    tN=selector();

                    	    state._fsp--;

                    	     expr.add(tN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop11;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:277:7: '\\*'
                    {
                    match(input,140,FOLLOW_140_in_selectClause945); 
                     expr = Collections.<RawSelector>emptyList();

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectClause"


    // $ANTLR start "selector"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:280:1: selector returns [RawSelector s] : us= unaliasedSelector ( K_AS c= cident )? ;
    public final RawSelector selector() throws RecognitionException {
        RawSelector s = null;

        Selectable us = null;

        ColumnIdentifier c = null;


         ColumnIdentifier alias = null; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:282:5: (us= unaliasedSelector ( K_AS c= cident )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:282:7: us= unaliasedSelector ( K_AS c= cident )?
            {
            pushFollow(FOLLOW_unaliasedSelector_in_selector978);
            us=unaliasedSelector();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:282:28: ( K_AS c= cident )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==K_AS) ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:282:29: K_AS c= cident
                    {
                    match(input,K_AS,FOLLOW_K_AS_in_selector981); 
                    pushFollow(FOLLOW_cident_in_selector985);
                    c=cident();

                    state._fsp--;

                     alias = c; 

                    }
                    break;

            }

             s = new RawSelector(us, alias); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return s;
    }
    // $ANTLR end "selector"


    // $ANTLR start "unaliasedSelector"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:285:1: unaliasedSelector returns [Selectable s] : (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )* ;
    public final Selectable unaliasedSelector() throws RecognitionException {
        Selectable s = null;

        ColumnIdentifier c = null;

        String f = null;

        List<Selectable> args = null;

        ColumnIdentifier fi = null;


         Selectable tmp = null; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:287:5: ( (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:287:8: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )*
            {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:287:8: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs )
            int alt14=4;
            alt14 = dfa14.predict(input);
            switch (alt14) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:287:10: c= cident
                    {
                    pushFollow(FOLLOW_cident_in_unaliasedSelector1026);
                    c=cident();

                    state._fsp--;

                     tmp = c; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:288:10: K_WRITETIME '(' c= cident ')'
                    {
                    match(input,K_WRITETIME,FOLLOW_K_WRITETIME_in_unaliasedSelector1072); 
                    match(input,137,FOLLOW_137_in_unaliasedSelector1074); 
                    pushFollow(FOLLOW_cident_in_unaliasedSelector1078);
                    c=cident();

                    state._fsp--;

                    match(input,138,FOLLOW_138_in_unaliasedSelector1080); 
                     tmp = new Selectable.WritetimeOrTTL(c, true); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:289:10: K_TTL '(' c= cident ')'
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_unaliasedSelector1106); 
                    match(input,137,FOLLOW_137_in_unaliasedSelector1114); 
                    pushFollow(FOLLOW_cident_in_unaliasedSelector1118);
                    c=cident();

                    state._fsp--;

                    match(input,138,FOLLOW_138_in_unaliasedSelector1120); 
                     tmp = new Selectable.WritetimeOrTTL(c, false); 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:290:10: f= functionName args= selectionFunctionArgs
                    {
                    pushFollow(FOLLOW_functionName_in_unaliasedSelector1148);
                    f=functionName();

                    state._fsp--;

                    pushFollow(FOLLOW_selectionFunctionArgs_in_unaliasedSelector1152);
                    args=selectionFunctionArgs();

                    state._fsp--;

                     tmp = new Selectable.WithFunction(f, args); 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:291:10: ( '.' fi= cident )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==141) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:291:12: '.' fi= cident
            	    {
            	    match(input,141,FOLLOW_141_in_unaliasedSelector1167); 
            	    pushFollow(FOLLOW_cident_in_unaliasedSelector1171);
            	    fi=cident();

            	    state._fsp--;

            	     tmp = new Selectable.WithFieldSelection(tmp, fi); 

            	    }
            	    break;

            	default :
            	    break loop15;
                }
            } while (true);

             s = tmp; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return s;
    }
    // $ANTLR end "unaliasedSelector"


    // $ANTLR start "selectionFunctionArgs"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:294:1: selectionFunctionArgs returns [List<Selectable> a] : ( '(' ')' | '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')' );
    public final List<Selectable> selectionFunctionArgs() throws RecognitionException {
        List<Selectable> a = null;

        Selectable s1 = null;

        Selectable sn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:295:5: ( '(' ')' | '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')' )
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==137) ) {
                int LA17_1 = input.LA(2);

                if ( (LA17_1==138) ) {
                    alt17=1;
                }
                else if ( ((LA17_1>=K_DISTINCT && LA17_1<=K_AS)||(LA17_1>=K_FILTERING && LA17_1<=K_TTL)||LA17_1==K_VALUES||LA17_1==K_EXISTS||LA17_1==K_TIMESTAMP||LA17_1==K_COUNTER||(LA17_1>=K_KEY && LA17_1<=K_CUSTOM)||LA17_1==IDENT||LA17_1==K_TRIGGER||LA17_1==K_LIST||(LA17_1>=K_ALL && LA17_1<=QUOTED_NAME)||LA17_1==K_TOKEN||(LA17_1>=K_CONTAINS && LA17_1<=K_MAP)) ) {
                    alt17=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 17, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;
            }
            switch (alt17) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:295:7: '(' ')'
                    {
                    match(input,137,FOLLOW_137_in_selectionFunctionArgs1199); 
                    match(input,138,FOLLOW_138_in_selectionFunctionArgs1201); 
                     a = Collections.emptyList(); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:296:7: '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')'
                    {
                    match(input,137,FOLLOW_137_in_selectionFunctionArgs1211); 
                    pushFollow(FOLLOW_unaliasedSelector_in_selectionFunctionArgs1215);
                    s1=unaliasedSelector();

                    state._fsp--;

                     List<Selectable> args = new ArrayList<Selectable>(); args.add(s1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:297:11: ( ',' sn= unaliasedSelector )*
                    loop16:
                    do {
                        int alt16=2;
                        int LA16_0 = input.LA(1);

                        if ( (LA16_0==139) ) {
                            alt16=1;
                        }


                        switch (alt16) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:297:13: ',' sn= unaliasedSelector
                    	    {
                    	    match(input,139,FOLLOW_139_in_selectionFunctionArgs1231); 
                    	    pushFollow(FOLLOW_unaliasedSelector_in_selectionFunctionArgs1235);
                    	    sn=unaliasedSelector();

                    	    state._fsp--;

                    	     args.add(sn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop16;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_selectionFunctionArgs1248); 
                     a = args; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return a;
    }
    // $ANTLR end "selectionFunctionArgs"


    // $ANTLR start "selectCountClause"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:301:1: selectCountClause returns [List<RawSelector> expr] : ( '\\*' | i= INTEGER );
    public final List<RawSelector> selectCountClause() throws RecognitionException {
        List<RawSelector> expr = null;

        Token i=null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:302:5: ( '\\*' | i= INTEGER )
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==140) ) {
                alt18=1;
            }
            else if ( (LA18_0==INTEGER) ) {
                alt18=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 18, 0, input);

                throw nvae;
            }
            switch (alt18) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:302:7: '\\*'
                    {
                    match(input,140,FOLLOW_140_in_selectCountClause1271); 
                     expr = Collections.<RawSelector>emptyList();

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:303:7: i= INTEGER
                    {
                    i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectCountClause1293); 
                     if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")"); expr = Collections.<RawSelector>emptyList();

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectCountClause"


    // $ANTLR start "whereClause"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:306:1: whereClause returns [List<Relation> clause] : relation[$clause] ( K_AND relation[$clause] )* ;
    public final List<Relation> whereClause() throws RecognitionException {
        List<Relation> clause = null;

         clause = new ArrayList<Relation>(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:308:5: ( relation[$clause] ( K_AND relation[$clause] )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:308:7: relation[$clause] ( K_AND relation[$clause] )*
            {
            pushFollow(FOLLOW_relation_in_whereClause1329);
            relation(clause);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:308:25: ( K_AND relation[$clause] )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==K_AND) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:308:26: K_AND relation[$clause]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_whereClause1333); 
            	    pushFollow(FOLLOW_relation_in_whereClause1335);
            	    relation(clause);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return clause;
    }
    // $ANTLR end "whereClause"


    // $ANTLR start "orderByClause"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:311:1: orderByClause[Map<ColumnIdentifier, Boolean> orderings] : c= cident ( K_ASC | K_DESC )? ;
    public final void orderByClause(Map<ColumnIdentifier, Boolean> orderings) throws RecognitionException {
        ColumnIdentifier c = null;



                ColumnIdentifier orderBy = null;
                boolean reversed = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:316:5: (c= cident ( K_ASC | K_DESC )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:316:7: c= cident ( K_ASC | K_DESC )?
            {
            pushFollow(FOLLOW_cident_in_orderByClause1366);
            c=cident();

            state._fsp--;

             orderBy = c; 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:316:33: ( K_ASC | K_DESC )?
            int alt20=3;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==K_ASC) ) {
                alt20=1;
            }
            else if ( (LA20_0==K_DESC) ) {
                alt20=2;
            }
            switch (alt20) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:316:34: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_orderByClause1371); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:316:42: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_orderByClause1375); 
                     reversed = true; 

                    }
                    break;

            }

             orderings.put(c, reversed); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "orderByClause"


    // $ANTLR start "insertStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:319:1: insertStatement returns [UpdateStatement.ParsedInsert expr] : K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )? ;
    public final UpdateStatement.ParsedInsert insertStatement() throws RecognitionException {
        UpdateStatement.ParsedInsert expr = null;

        CFName cf = null;

        ColumnIdentifier c1 = null;

        ColumnIdentifier cn = null;

        Term.Raw v1 = null;

        Term.Raw vn = null;



                Attributes.Raw attrs = new Attributes.Raw();
                List<ColumnIdentifier> columnNames  = new ArrayList<ColumnIdentifier>();
                List<Term.Raw> values = new ArrayList<Term.Raw>();
                boolean ifNotExists = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:332:5: ( K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:332:7: K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )?
            {
            match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement1413); 
            match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement1415); 
            pushFollow(FOLLOW_columnFamilyName_in_insertStatement1419);
            cf=columnFamilyName();

            state._fsp--;

            match(input,137,FOLLOW_137_in_insertStatement1431); 
            pushFollow(FOLLOW_cident_in_insertStatement1435);
            c1=cident();

            state._fsp--;

             columnNames.add(c1); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:333:51: ( ',' cn= cident )*
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( (LA21_0==139) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:333:53: ',' cn= cident
            	    {
            	    match(input,139,FOLLOW_139_in_insertStatement1442); 
            	    pushFollow(FOLLOW_cident_in_insertStatement1446);
            	    cn=cident();

            	    state._fsp--;

            	     columnNames.add(cn); 

            	    }
            	    break;

            	default :
            	    break loop21;
                }
            } while (true);

            match(input,138,FOLLOW_138_in_insertStatement1453); 
            match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement1463); 
            match(input,137,FOLLOW_137_in_insertStatement1475); 
            pushFollow(FOLLOW_term_in_insertStatement1479);
            v1=term();

            state._fsp--;

             values.add(v1); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:335:43: ( ',' vn= term )*
            loop22:
            do {
                int alt22=2;
                int LA22_0 = input.LA(1);

                if ( (LA22_0==139) ) {
                    alt22=1;
                }


                switch (alt22) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:335:45: ',' vn= term
            	    {
            	    match(input,139,FOLLOW_139_in_insertStatement1485); 
            	    pushFollow(FOLLOW_term_in_insertStatement1489);
            	    vn=term();

            	    state._fsp--;

            	     values.add(vn); 

            	    }
            	    break;

            	default :
            	    break loop22;
                }
            } while (true);

            match(input,138,FOLLOW_138_in_insertStatement1496); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:337:9: ( K_IF K_NOT K_EXISTS )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==K_IF) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:337:11: K_IF K_NOT K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_insertStatement1509); 
                    match(input,K_NOT,FOLLOW_K_NOT_in_insertStatement1511); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_insertStatement1513); 
                     ifNotExists = true; 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:338:9: ( usingClause[attrs] )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==K_USING) ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:338:11: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_insertStatement1530);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }


                      expr = new UpdateStatement.ParsedInsert(cf,
                                                               attrs,
                                                               columnNames,
                                                               values,
                                                               ifNotExists);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "insertStatement"


    // $ANTLR start "usingClause"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:348:1: usingClause[Attributes.Raw attrs] : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )* ;
    public final void usingClause(Attributes.Raw attrs) throws RecognitionException {
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:349:5: ( K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:349:7: K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClause1560); 
            pushFollow(FOLLOW_usingClauseObjective_in_usingClause1562);
            usingClauseObjective(attrs);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:349:43: ( K_AND usingClauseObjective[attrs] )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==K_AND) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:349:45: K_AND usingClauseObjective[attrs]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_usingClause1567); 
            	    pushFollow(FOLLOW_usingClauseObjective_in_usingClause1569);
            	    usingClauseObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClause"


    // $ANTLR start "usingClauseObjective"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:352:1: usingClauseObjective[Attributes.Raw attrs] : ( K_TIMESTAMP ts= intValue | K_TTL t= intValue );
    public final void usingClauseObjective(Attributes.Raw attrs) throws RecognitionException {
        Term.Raw ts = null;

        Term.Raw t = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:353:5: ( K_TIMESTAMP ts= intValue | K_TTL t= intValue )
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==K_TIMESTAMP) ) {
                alt26=1;
            }
            else if ( (LA26_0==K_TTL) ) {
                alt26=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;
            }
            switch (alt26) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:353:7: K_TIMESTAMP ts= intValue
                    {
                    match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseObjective1591); 
                    pushFollow(FOLLOW_intValue_in_usingClauseObjective1595);
                    ts=intValue();

                    state._fsp--;

                     attrs.timestamp = ts; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:354:7: K_TTL t= intValue
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective1605); 
                    pushFollow(FOLLOW_intValue_in_usingClauseObjective1609);
                    t=intValue();

                    state._fsp--;

                     attrs.timeToLive = t; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseObjective"


    // $ANTLR start "updateStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:357:1: updateStatement returns [UpdateStatement.ParsedUpdate expr] : K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )? ;
    public final UpdateStatement.ParsedUpdate updateStatement() throws RecognitionException {
        UpdateStatement.ParsedUpdate expr = null;

        CFName cf = null;

        List<Relation> wclause = null;

        List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions = null;



                Attributes.Raw attrs = new Attributes.Raw();
                List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations = new ArrayList<Pair<ColumnIdentifier, Operation.RawUpdate>>();
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:368:5: ( K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:368:7: K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )?
            {
            match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1643); 
            pushFollow(FOLLOW_columnFamilyName_in_updateStatement1647);
            cf=columnFamilyName();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:369:7: ( usingClause[attrs] )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==K_USING) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:369:9: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_updateStatement1657);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_SET,FOLLOW_K_SET_in_updateStatement1669); 
            pushFollow(FOLLOW_columnOperation_in_updateStatement1671);
            columnOperation(operations);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:370:41: ( ',' columnOperation[operations] )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( (LA28_0==139) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:370:42: ',' columnOperation[operations]
            	    {
            	    match(input,139,FOLLOW_139_in_updateStatement1675); 
            	    pushFollow(FOLLOW_columnOperation_in_updateStatement1677);
            	    columnOperation(operations);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop28;
                }
            } while (true);

            match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1688); 
            pushFollow(FOLLOW_whereClause_in_updateStatement1692);
            wclause=whereClause();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:372:7: ( K_IF conditions= updateCondition )?
            int alt29=2;
            int LA29_0 = input.LA(1);

            if ( (LA29_0==K_IF) ) {
                alt29=1;
            }
            switch (alt29) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:372:9: K_IF conditions= updateCondition
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_updateStatement1702); 
                    pushFollow(FOLLOW_updateCondition_in_updateStatement1706);
                    conditions=updateCondition();

                    state._fsp--;


                    }
                    break;

            }


                      return new UpdateStatement.ParsedUpdate(cf,
                                                              attrs,
                                                              operations,
                                                              wclause,
                                                              conditions == null ? Collections.<Pair<ColumnIdentifier, Operation.RawUpdate>>emptyList() : conditions);
                 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "updateStatement"


    // $ANTLR start "updateCondition"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:382:1: updateCondition returns [List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions] : columnOperation[conditions] ( K_AND columnOperation[conditions] )* ;
    public final List<Pair<ColumnIdentifier, Operation.RawUpdate>> updateCondition() throws RecognitionException {
        List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions = null;

         conditions = new ArrayList<Pair<ColumnIdentifier, Operation.RawUpdate>>(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:384:5: ( columnOperation[conditions] ( K_AND columnOperation[conditions] )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:384:7: columnOperation[conditions] ( K_AND columnOperation[conditions] )*
            {
            pushFollow(FOLLOW_columnOperation_in_updateCondition1747);
            columnOperation(conditions);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:384:35: ( K_AND columnOperation[conditions] )*
            loop30:
            do {
                int alt30=2;
                int LA30_0 = input.LA(1);

                if ( (LA30_0==K_AND) ) {
                    alt30=1;
                }


                switch (alt30) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:384:37: K_AND columnOperation[conditions]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_updateCondition1752); 
            	    pushFollow(FOLLOW_columnOperation_in_updateCondition1754);
            	    columnOperation(conditions);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop30;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return conditions;
    }
    // $ANTLR end "updateCondition"


    // $ANTLR start "deleteStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:387:1: deleteStatement returns [DeleteStatement.Parsed expr] : K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )? ;
    public final DeleteStatement.Parsed deleteStatement() throws RecognitionException {
        DeleteStatement.Parsed expr = null;

        List<Operation.RawDeletion> dels = null;

        CFName cf = null;

        List<Relation> wclause = null;

        List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions = null;



                Attributes.Raw attrs = new Attributes.Raw();
                List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:398:5: ( K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:398:7: K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF conditions= updateCondition )?
            {
            match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1790); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:398:16: (dels= deleteSelection )?
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( ((LA31_0>=K_DISTINCT && LA31_0<=K_AS)||(LA31_0>=K_FILTERING && LA31_0<=K_TTL)||LA31_0==K_VALUES||LA31_0==K_EXISTS||LA31_0==K_TIMESTAMP||LA31_0==K_COUNTER||(LA31_0>=K_KEY && LA31_0<=K_CUSTOM)||LA31_0==IDENT||LA31_0==K_TRIGGER||LA31_0==K_LIST||(LA31_0>=K_ALL && LA31_0<=QUOTED_NAME)||(LA31_0>=K_CONTAINS && LA31_0<=K_MAP)) ) {
                alt31=1;
            }
            switch (alt31) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:398:18: dels= deleteSelection
                    {
                    pushFollow(FOLLOW_deleteSelection_in_deleteStatement1796);
                    dels=deleteSelection();

                    state._fsp--;

                     columnDeletions = dels; 

                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1809); 
            pushFollow(FOLLOW_columnFamilyName_in_deleteStatement1813);
            cf=columnFamilyName();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:400:7: ( usingClauseDelete[attrs] )?
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==K_USING) ) {
                alt32=1;
            }
            switch (alt32) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:400:9: usingClauseDelete[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1823);
                    usingClauseDelete(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1835); 
            pushFollow(FOLLOW_whereClause_in_deleteStatement1839);
            wclause=whereClause();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:402:7: ( K_IF conditions= updateCondition )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==K_IF) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:402:9: K_IF conditions= updateCondition
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_deleteStatement1849); 
                    pushFollow(FOLLOW_updateCondition_in_deleteStatement1853);
                    conditions=updateCondition();

                    state._fsp--;


                    }
                    break;

            }


                      return new DeleteStatement.Parsed(cf,
                                                        attrs,
                                                        columnDeletions,
                                                        wclause,
                                                        conditions == null ? Collections.<Pair<ColumnIdentifier, Operation.RawUpdate>>emptyList() : conditions);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "deleteStatement"


    // $ANTLR start "deleteSelection"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:412:1: deleteSelection returns [List<Operation.RawDeletion> operations] : t1= deleteOp ( ',' tN= deleteOp )* ;
    public final List<Operation.RawDeletion> deleteSelection() throws RecognitionException {
        List<Operation.RawDeletion> operations = null;

        Operation.RawDeletion t1 = null;

        Operation.RawDeletion tN = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:413:5: (t1= deleteOp ( ',' tN= deleteOp )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:413:7: t1= deleteOp ( ',' tN= deleteOp )*
            {
             operations = new ArrayList<Operation.RawDeletion>(); 
            pushFollow(FOLLOW_deleteOp_in_deleteSelection1899);
            t1=deleteOp();

            state._fsp--;

             operations.add(t1); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:415:11: ( ',' tN= deleteOp )*
            loop34:
            do {
                int alt34=2;
                int LA34_0 = input.LA(1);

                if ( (LA34_0==139) ) {
                    alt34=1;
                }


                switch (alt34) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:415:12: ',' tN= deleteOp
            	    {
            	    match(input,139,FOLLOW_139_in_deleteSelection1914); 
            	    pushFollow(FOLLOW_deleteOp_in_deleteSelection1918);
            	    tN=deleteOp();

            	    state._fsp--;

            	     operations.add(tN); 

            	    }
            	    break;

            	default :
            	    break loop34;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return operations;
    }
    // $ANTLR end "deleteSelection"


    // $ANTLR start "deleteOp"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:418:1: deleteOp returns [Operation.RawDeletion op] : (c= cident | c= cident '[' t= term ']' );
    public final Operation.RawDeletion deleteOp() throws RecognitionException {
        Operation.RawDeletion op = null;

        ColumnIdentifier c = null;

        Term.Raw t = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:419:5: (c= cident | c= cident '[' t= term ']' )
            int alt35=2;
            alt35 = dfa35.predict(input);
            switch (alt35) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:419:7: c= cident
                    {
                    pushFollow(FOLLOW_cident_in_deleteOp1945);
                    c=cident();

                    state._fsp--;

                     op = new Operation.ColumnDeletion(c); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:420:7: c= cident '[' t= term ']'
                    {
                    pushFollow(FOLLOW_cident_in_deleteOp1972);
                    c=cident();

                    state._fsp--;

                    match(input,142,FOLLOW_142_in_deleteOp1974); 
                    pushFollow(FOLLOW_term_in_deleteOp1978);
                    t=term();

                    state._fsp--;

                    match(input,143,FOLLOW_143_in_deleteOp1980); 
                     op = new Operation.ElementDeletion(c, t); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return op;
    }
    // $ANTLR end "deleteOp"


    // $ANTLR start "usingClauseDelete"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:423:1: usingClauseDelete[Attributes.Raw attrs] : K_USING K_TIMESTAMP ts= intValue ;
    public final void usingClauseDelete(Attributes.Raw attrs) throws RecognitionException {
        Term.Raw ts = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:424:5: ( K_USING K_TIMESTAMP ts= intValue )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:424:7: K_USING K_TIMESTAMP ts= intValue
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete2000); 
            match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDelete2002); 
            pushFollow(FOLLOW_intValue_in_usingClauseDelete2006);
            ts=intValue();

            state._fsp--;

             attrs.timestamp = ts; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseDelete"


    // $ANTLR start "batchStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:427:1: batchStatement returns [BatchStatement.Parsed expr] : K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH ;
    public final BatchStatement.Parsed batchStatement() throws RecognitionException {
        BatchStatement.Parsed expr = null;

        ModificationStatement.Parsed s = null;



                BatchStatement.Type type = BatchStatement.Type.LOGGED;
                List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
                Attributes.Raw attrs = new Attributes.Raw();
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:457:5: ( K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:457:7: K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH
            {
            match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement2040); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:458:7: ( K_UNLOGGED | K_COUNTER )?
            int alt36=3;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==K_UNLOGGED) ) {
                alt36=1;
            }
            else if ( (LA36_0==K_COUNTER) ) {
                alt36=2;
            }
            switch (alt36) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:458:9: K_UNLOGGED
                    {
                    match(input,K_UNLOGGED,FOLLOW_K_UNLOGGED_in_batchStatement2050); 
                     type = BatchStatement.Type.UNLOGGED; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:458:63: K_COUNTER
                    {
                    match(input,K_COUNTER,FOLLOW_K_COUNTER_in_batchStatement2056); 
                     type = BatchStatement.Type.COUNTER; 

                    }
                    break;

            }

            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement2069); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:459:15: ( usingClause[attrs] )?
            int alt37=2;
            int LA37_0 = input.LA(1);

            if ( (LA37_0==K_USING) ) {
                alt37=1;
            }
            switch (alt37) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:459:17: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_batchStatement2073);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:460:11: (s= batchStatementObjective ( ';' )? )*
            loop39:
            do {
                int alt39=2;
                int LA39_0 = input.LA(1);

                if ( (LA39_0==K_INSERT||LA39_0==K_UPDATE||LA39_0==K_DELETE) ) {
                    alt39=1;
                }


                switch (alt39) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:460:13: s= batchStatementObjective ( ';' )?
            	    {
            	    pushFollow(FOLLOW_batchStatementObjective_in_batchStatement2093);
            	    s=batchStatementObjective();

            	    state._fsp--;

            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:460:39: ( ';' )?
            	    int alt38=2;
            	    int LA38_0 = input.LA(1);

            	    if ( (LA38_0==136) ) {
            	        alt38=1;
            	    }
            	    switch (alt38) {
            	        case 1 :
            	            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:460:39: ';'
            	            {
            	            match(input,136,FOLLOW_136_in_batchStatement2095); 

            	            }
            	            break;

            	    }

            	     statements.add(s); 

            	    }
            	    break;

            	default :
            	    break loop39;
                }
            } while (true);

            match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement2109); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement2111); 

                      return new BatchStatement.Parsed(type, attrs, statements);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "batchStatement"


    // $ANTLR start "batchStatementObjective"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:467:1: batchStatementObjective returns [ModificationStatement.Parsed statement] : (i= insertStatement | u= updateStatement | d= deleteStatement );
    public final ModificationStatement.Parsed batchStatementObjective() throws RecognitionException {
        ModificationStatement.Parsed statement = null;

        UpdateStatement.ParsedInsert i = null;

        UpdateStatement.ParsedUpdate u = null;

        DeleteStatement.Parsed d = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:468:5: (i= insertStatement | u= updateStatement | d= deleteStatement )
            int alt40=3;
            switch ( input.LA(1) ) {
            case K_INSERT:
                {
                alt40=1;
                }
                break;
            case K_UPDATE:
                {
                alt40=2;
                }
                break;
            case K_DELETE:
                {
                alt40=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 40, 0, input);

                throw nvae;
            }

            switch (alt40) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:468:7: i= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_batchStatementObjective2142);
                    i=insertStatement();

                    state._fsp--;

                     statement = i; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:469:7: u= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_batchStatementObjective2155);
                    u=updateStatement();

                    state._fsp--;

                     statement = u; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:470:7: d= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective2168);
                    d=deleteStatement();

                    state._fsp--;

                     statement = d; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return statement;
    }
    // $ANTLR end "batchStatementObjective"


    // $ANTLR start "createKeyspaceStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:473:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs] ;
    public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
        CreateKeyspaceStatement expr = null;

        String ks = null;



                KSPropDefs attrs = new KSPropDefs();
                boolean ifNotExists = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:481:5: ( K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:481:7: K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement2203); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement2205); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:481:27: ( K_IF K_NOT K_EXISTS )?
            int alt41=2;
            int LA41_0 = input.LA(1);

            if ( (LA41_0==K_IF) ) {
                alt41=1;
            }
            switch (alt41) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:481:28: K_IF K_NOT K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_createKeyspaceStatement2208); 
                    match(input,K_NOT,FOLLOW_K_NOT_in_createKeyspaceStatement2210); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createKeyspaceStatement2212); 
                     ifNotExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_keyspaceName_in_createKeyspaceStatement2221);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement2229); 
            pushFollow(FOLLOW_properties_in_createKeyspaceStatement2231);
            properties(attrs);

            state._fsp--;

             expr = new CreateKeyspaceStatement(ks, attrs, ifNotExists); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createKeyspaceStatement"


    // $ANTLR start "createTableStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:485:1: createTableStatement returns [CreateTableStatement.RawStatement expr] : K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr] ;
    public final CreateTableStatement.RawStatement createTableStatement() throws RecognitionException {
        CreateTableStatement.RawStatement expr = null;

        CFName cf = null;


         boolean ifNotExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:494:5: ( K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:494:7: K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createTableStatement2266); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createTableStatement2268); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:494:31: ( K_IF K_NOT K_EXISTS )?
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==K_IF) ) {
                alt42=1;
            }
            switch (alt42) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:494:32: K_IF K_NOT K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_createTableStatement2271); 
                    match(input,K_NOT,FOLLOW_K_NOT_in_createTableStatement2273); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createTableStatement2275); 
                     ifNotExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_columnFamilyName_in_createTableStatement2290);
            cf=columnFamilyName();

            state._fsp--;

             expr = new CreateTableStatement.RawStatement(cf, ifNotExists); 
            pushFollow(FOLLOW_cfamDefinition_in_createTableStatement2300);
            cfamDefinition(expr);

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createTableStatement"


    // $ANTLR start "cfamDefinition"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:499:1: cfamDefinition[CreateTableStatement.RawStatement expr] : '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? ;
    public final void cfamDefinition(CreateTableStatement.RawStatement expr) throws RecognitionException {
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:5: ( '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:7: '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            {
            match(input,137,FOLLOW_137_in_cfamDefinition2319); 
            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition2321);
            cfamColumns(expr);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:29: ( ',' ( cfamColumns[expr] )? )*
            loop44:
            do {
                int alt44=2;
                int LA44_0 = input.LA(1);

                if ( (LA44_0==139) ) {
                    alt44=1;
                }


                switch (alt44) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:31: ',' ( cfamColumns[expr] )?
            	    {
            	    match(input,139,FOLLOW_139_in_cfamDefinition2326); 
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:35: ( cfamColumns[expr] )?
            	    int alt43=2;
            	    int LA43_0 = input.LA(1);

            	    if ( ((LA43_0>=K_DISTINCT && LA43_0<=K_AS)||(LA43_0>=K_FILTERING && LA43_0<=K_TTL)||LA43_0==K_VALUES||LA43_0==K_EXISTS||LA43_0==K_TIMESTAMP||LA43_0==K_COUNTER||(LA43_0>=K_PRIMARY && LA43_0<=K_CUSTOM)||LA43_0==IDENT||LA43_0==K_TRIGGER||LA43_0==K_LIST||(LA43_0>=K_ALL && LA43_0<=QUOTED_NAME)||(LA43_0>=K_CONTAINS && LA43_0<=K_MAP)) ) {
            	        alt43=1;
            	    }
            	    switch (alt43) {
            	        case 1 :
            	            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:500:35: cfamColumns[expr]
            	            {
            	            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition2328);
            	            cfamColumns(expr);

            	            state._fsp--;


            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    break loop44;
                }
            } while (true);

            match(input,138,FOLLOW_138_in_cfamDefinition2335); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:501:7: ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==K_WITH) ) {
                alt46=1;
            }
            switch (alt46) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:501:9: K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_cfamDefinition2345); 
                    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2347);
                    cfamProperty(expr);

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:501:35: ( K_AND cfamProperty[expr] )*
                    loop45:
                    do {
                        int alt45=2;
                        int LA45_0 = input.LA(1);

                        if ( (LA45_0==K_AND) ) {
                            alt45=1;
                        }


                        switch (alt45) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:501:37: K_AND cfamProperty[expr]
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_cfamDefinition2352); 
                    	    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2354);
                    	    cfamProperty(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop45;
                        }
                    } while (true);


                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamDefinition"


    // $ANTLR start "cfamColumns"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:504:1: cfamColumns[CreateTableStatement.RawStatement expr] : (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')' );
    public final void cfamColumns(CreateTableStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        CQL3Type v = null;

        ColumnIdentifier c = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:505:5: (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')' )
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( ((LA49_0>=K_DISTINCT && LA49_0<=K_AS)||(LA49_0>=K_FILTERING && LA49_0<=K_TTL)||LA49_0==K_VALUES||LA49_0==K_EXISTS||LA49_0==K_TIMESTAMP||LA49_0==K_COUNTER||(LA49_0>=K_KEY && LA49_0<=K_CUSTOM)||LA49_0==IDENT||LA49_0==K_TRIGGER||LA49_0==K_LIST||(LA49_0>=K_ALL && LA49_0<=QUOTED_NAME)||(LA49_0>=K_CONTAINS && LA49_0<=K_MAP)) ) {
                alt49=1;
            }
            else if ( (LA49_0==K_PRIMARY) ) {
                alt49=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 49, 0, input);

                throw nvae;
            }
            switch (alt49) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:505:7: k= cident v= comparatorType ( K_PRIMARY K_KEY )?
                    {
                    pushFollow(FOLLOW_cident_in_cfamColumns2380);
                    k=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_cfamColumns2384);
                    v=comparatorType();

                    state._fsp--;

                     expr.addDefinition(k, v); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:505:64: ( K_PRIMARY K_KEY )?
                    int alt47=2;
                    int LA47_0 = input.LA(1);

                    if ( (LA47_0==K_PRIMARY) ) {
                        alt47=1;
                    }
                    switch (alt47) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:505:65: K_PRIMARY K_KEY
                            {
                            match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2389); 
                            match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2391); 
                             expr.addKeyAliases(Collections.singletonList(k)); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:506:7: K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')'
                    {
                    match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2403); 
                    match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2405); 
                    match(input,137,FOLLOW_137_in_cfamColumns2407); 
                    pushFollow(FOLLOW_pkDef_in_cfamColumns2409);
                    pkDef(expr);

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:506:39: ( ',' c= cident )*
                    loop48:
                    do {
                        int alt48=2;
                        int LA48_0 = input.LA(1);

                        if ( (LA48_0==139) ) {
                            alt48=1;
                        }


                        switch (alt48) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:506:40: ',' c= cident
                    	    {
                    	    match(input,139,FOLLOW_139_in_cfamColumns2413); 
                    	    pushFollow(FOLLOW_cident_in_cfamColumns2417);
                    	    c=cident();

                    	    state._fsp--;

                    	     expr.addColumnAlias(c); 

                    	    }
                    	    break;

                    	default :
                    	    break loop48;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_cfamColumns2424); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamColumns"


    // $ANTLR start "pkDef"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:509:1: pkDef[CreateTableStatement.RawStatement expr] : (k= cident | '(' k1= cident ( ',' kn= cident )* ')' );
    public final void pkDef(CreateTableStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        ColumnIdentifier k1 = null;

        ColumnIdentifier kn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:510:5: (k= cident | '(' k1= cident ( ',' kn= cident )* ')' )
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( ((LA51_0>=K_DISTINCT && LA51_0<=K_AS)||(LA51_0>=K_FILTERING && LA51_0<=K_TTL)||LA51_0==K_VALUES||LA51_0==K_EXISTS||LA51_0==K_TIMESTAMP||LA51_0==K_COUNTER||(LA51_0>=K_KEY && LA51_0<=K_CUSTOM)||LA51_0==IDENT||LA51_0==K_TRIGGER||LA51_0==K_LIST||(LA51_0>=K_ALL && LA51_0<=QUOTED_NAME)||(LA51_0>=K_CONTAINS && LA51_0<=K_MAP)) ) {
                alt51=1;
            }
            else if ( (LA51_0==137) ) {
                alt51=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 51, 0, input);

                throw nvae;
            }
            switch (alt51) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:510:7: k= cident
                    {
                    pushFollow(FOLLOW_cident_in_pkDef2444);
                    k=cident();

                    state._fsp--;

                     expr.addKeyAliases(Collections.singletonList(k)); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:511:7: '(' k1= cident ( ',' kn= cident )* ')'
                    {
                    match(input,137,FOLLOW_137_in_pkDef2454); 
                     List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); 
                    pushFollow(FOLLOW_cident_in_pkDef2460);
                    k1=cident();

                    state._fsp--;

                     l.add(k1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:511:102: ( ',' kn= cident )*
                    loop50:
                    do {
                        int alt50=2;
                        int LA50_0 = input.LA(1);

                        if ( (LA50_0==139) ) {
                            alt50=1;
                        }


                        switch (alt50) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:511:104: ',' kn= cident
                    	    {
                    	    match(input,139,FOLLOW_139_in_pkDef2466); 
                    	    pushFollow(FOLLOW_cident_in_pkDef2470);
                    	    kn=cident();

                    	    state._fsp--;

                    	     l.add(kn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop50;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_pkDef2477); 
                     expr.addKeyAliases(l); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "pkDef"


    // $ANTLR start "cfamProperty"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:514:1: cfamProperty[CreateTableStatement.RawStatement expr] : ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' );
    public final void cfamProperty(CreateTableStatement.RawStatement expr) throws RecognitionException {
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:515:5: ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' )
            int alt53=3;
            switch ( input.LA(1) ) {
            case K_DISTINCT:
            case K_COUNT:
            case K_AS:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_STORAGE:
            case K_TYPE:
            case K_CUSTOM:
            case IDENT:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case QUOTED_NAME:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt53=1;
                }
                break;
            case K_COMPACT:
                {
                int LA53_2 = input.LA(2);

                if ( (LA53_2==K_STORAGE) ) {
                    alt53=2;
                }
                else if ( (LA53_2==148) ) {
                    alt53=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 53, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_CLUSTERING:
                {
                int LA53_3 = input.LA(2);

                if ( (LA53_3==K_ORDER) ) {
                    alt53=3;
                }
                else if ( (LA53_3==148) ) {
                    alt53=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 53, 3, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 53, 0, input);

                throw nvae;
            }

            switch (alt53) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:515:7: property[expr.properties]
                    {
                    pushFollow(FOLLOW_property_in_cfamProperty2497);
                    property(expr.properties);

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:516:7: K_COMPACT K_STORAGE
                    {
                    match(input,K_COMPACT,FOLLOW_K_COMPACT_in_cfamProperty2506); 
                    match(input,K_STORAGE,FOLLOW_K_STORAGE_in_cfamProperty2508); 
                     expr.setCompactStorage(); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:517:7: K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')'
                    {
                    match(input,K_CLUSTERING,FOLLOW_K_CLUSTERING_in_cfamProperty2518); 
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_cfamProperty2520); 
                    match(input,K_BY,FOLLOW_K_BY_in_cfamProperty2522); 
                    match(input,137,FOLLOW_137_in_cfamProperty2524); 
                    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2526);
                    cfamOrdering(expr);

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:517:56: ( ',' cfamOrdering[expr] )*
                    loop52:
                    do {
                        int alt52=2;
                        int LA52_0 = input.LA(1);

                        if ( (LA52_0==139) ) {
                            alt52=1;
                        }


                        switch (alt52) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:517:57: ',' cfamOrdering[expr]
                    	    {
                    	    match(input,139,FOLLOW_139_in_cfamProperty2530); 
                    	    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2532);
                    	    cfamOrdering(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop52;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_cfamProperty2537); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamProperty"


    // $ANTLR start "cfamOrdering"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:520:1: cfamOrdering[CreateTableStatement.RawStatement expr] : k= cident ( K_ASC | K_DESC ) ;
    public final void cfamOrdering(CreateTableStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;


         boolean reversed=false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:522:5: (k= cident ( K_ASC | K_DESC ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:522:7: k= cident ( K_ASC | K_DESC )
            {
            pushFollow(FOLLOW_cident_in_cfamOrdering2565);
            k=cident();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:522:16: ( K_ASC | K_DESC )
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==K_ASC) ) {
                alt54=1;
            }
            else if ( (LA54_0==K_DESC) ) {
                alt54=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 54, 0, input);

                throw nvae;
            }
            switch (alt54) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:522:17: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_cfamOrdering2568); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:522:25: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_cfamOrdering2572); 
                     reversed=true;

                    }
                    break;

            }

             expr.setOrdering(k, reversed); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamOrdering"


    // $ANTLR start "createTypeStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:526:1: createTypeStatement returns [CreateTypeStatement expr] : K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= non_type_ident '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')' ;
    public final CreateTypeStatement createTypeStatement() throws RecognitionException {
        CreateTypeStatement expr = null;

        ColumnIdentifier tn = null;


         boolean ifNotExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:535:5: ( K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= non_type_ident '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')' )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:535:7: K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= non_type_ident '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')'
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createTypeStatement2611); 
            match(input,K_TYPE,FOLLOW_K_TYPE_in_createTypeStatement2613); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:535:23: ( K_IF K_NOT K_EXISTS )?
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==K_IF) ) {
                alt55=1;
            }
            switch (alt55) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:535:24: K_IF K_NOT K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_createTypeStatement2616); 
                    match(input,K_NOT,FOLLOW_K_NOT_in_createTypeStatement2618); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createTypeStatement2620); 
                     ifNotExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_non_type_ident_in_createTypeStatement2638);
            tn=non_type_ident();

            state._fsp--;

             expr = new CreateTypeStatement(tn, ifNotExists); 
            match(input,137,FOLLOW_137_in_createTypeStatement2651); 
            pushFollow(FOLLOW_typeColumns_in_createTypeStatement2653);
            typeColumns(expr);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:537:32: ( ',' ( typeColumns[expr] )? )*
            loop57:
            do {
                int alt57=2;
                int LA57_0 = input.LA(1);

                if ( (LA57_0==139) ) {
                    alt57=1;
                }


                switch (alt57) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:537:34: ',' ( typeColumns[expr] )?
            	    {
            	    match(input,139,FOLLOW_139_in_createTypeStatement2658); 
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:537:38: ( typeColumns[expr] )?
            	    int alt56=2;
            	    int LA56_0 = input.LA(1);

            	    if ( ((LA56_0>=K_DISTINCT && LA56_0<=K_AS)||(LA56_0>=K_FILTERING && LA56_0<=K_TTL)||LA56_0==K_VALUES||LA56_0==K_EXISTS||LA56_0==K_TIMESTAMP||LA56_0==K_COUNTER||(LA56_0>=K_KEY && LA56_0<=K_CUSTOM)||LA56_0==IDENT||LA56_0==K_TRIGGER||LA56_0==K_LIST||(LA56_0>=K_ALL && LA56_0<=QUOTED_NAME)||(LA56_0>=K_CONTAINS && LA56_0<=K_MAP)) ) {
            	        alt56=1;
            	    }
            	    switch (alt56) {
            	        case 1 :
            	            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:537:38: typeColumns[expr]
            	            {
            	            pushFollow(FOLLOW_typeColumns_in_createTypeStatement2660);
            	            typeColumns(expr);

            	            state._fsp--;


            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    break loop57;
                }
            } while (true);

            match(input,138,FOLLOW_138_in_createTypeStatement2667); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createTypeStatement"


    // $ANTLR start "typeColumns"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:540:1: typeColumns[CreateTypeStatement expr] : k= cident v= comparatorType ;
    public final void typeColumns(CreateTypeStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        CQL3Type v = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:541:5: (k= cident v= comparatorType )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:541:7: k= cident v= comparatorType
            {
            pushFollow(FOLLOW_cident_in_typeColumns2687);
            k=cident();

            state._fsp--;

            pushFollow(FOLLOW_comparatorType_in_typeColumns2691);
            v=comparatorType();

            state._fsp--;

             expr.addDefinition(k, v); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "typeColumns"


    // $ANTLR start "createIndexStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:545:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' ( K_USING cls= STRING_LITERAL )? ;
    public final CreateIndexStatement createIndexStatement() throws RecognitionException {
        CreateIndexStatement expr = null;

        Token idxName=null;
        Token cls=null;
        CFName cf = null;

        ColumnIdentifier id = null;



                boolean isCustom = false;
                boolean ifNotExists = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:5: ( K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' ( K_USING cls= STRING_LITERAL )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:7: K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' ( K_USING cls= STRING_LITERAL )?
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement2726); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:16: ( K_CUSTOM )?
            int alt58=2;
            int LA58_0 = input.LA(1);

            if ( (LA58_0==K_CUSTOM) ) {
                alt58=1;
            }
            switch (alt58) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:17: K_CUSTOM
                    {
                    match(input,K_CUSTOM,FOLLOW_K_CUSTOM_in_createIndexStatement2729); 
                     isCustom = true; 

                    }
                    break;

            }

            match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement2735); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:57: ( K_IF K_NOT K_EXISTS )?
            int alt59=2;
            int LA59_0 = input.LA(1);

            if ( (LA59_0==K_IF) ) {
                alt59=1;
            }
            switch (alt59) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:554:58: K_IF K_NOT K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_createIndexStatement2738); 
                    match(input,K_NOT,FOLLOW_K_NOT_in_createIndexStatement2740); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createIndexStatement2742); 
                     ifNotExists = true; 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:555:9: (idxName= IDENT )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==IDENT) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:555:10: idxName= IDENT
                    {
                    idxName=(Token)match(input,IDENT,FOLLOW_IDENT_in_createIndexStatement2760); 

                    }
                    break;

            }

            match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement2764); 
            pushFollow(FOLLOW_columnFamilyName_in_createIndexStatement2768);
            cf=columnFamilyName();

            state._fsp--;

            match(input,137,FOLLOW_137_in_createIndexStatement2770); 
            pushFollow(FOLLOW_cident_in_createIndexStatement2774);
            id=cident();

            state._fsp--;

            match(input,138,FOLLOW_138_in_createIndexStatement2776); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:556:9: ( K_USING cls= STRING_LITERAL )?
            int alt61=2;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==K_USING) ) {
                alt61=1;
            }
            switch (alt61) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:556:11: K_USING cls= STRING_LITERAL
                    {
                    match(input,K_USING,FOLLOW_K_USING_in_createIndexStatement2788); 
                    cls=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createIndexStatement2792); 

                    }
                    break;

            }

             expr = new CreateIndexStatement(cf, (idxName!=null?idxName.getText():null), id, ifNotExists, isCustom, (cls!=null?cls.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createIndexStatement"


    // $ANTLR start "createTriggerStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:560:1: createTriggerStatement returns [CreateTriggerStatement expr] : K_CREATE K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL ;
    public final CreateTriggerStatement createTriggerStatement() throws RecognitionException {
        CreateTriggerStatement expr = null;

        Token name=null;
        Token cls=null;
        CFName cf = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:564:5: ( K_CREATE K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:564:7: K_CREATE K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createTriggerStatement2826); 
            match(input,K_TRIGGER,FOLLOW_K_TRIGGER_in_createTriggerStatement2828); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:564:26: (name= IDENT )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:564:27: name= IDENT
            {
            name=(Token)match(input,IDENT,FOLLOW_IDENT_in_createTriggerStatement2833); 

            }

            match(input,K_ON,FOLLOW_K_ON_in_createTriggerStatement2836); 
            pushFollow(FOLLOW_columnFamilyName_in_createTriggerStatement2840);
            cf=columnFamilyName();

            state._fsp--;

            match(input,K_USING,FOLLOW_K_USING_in_createTriggerStatement2842); 
            cls=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createTriggerStatement2846); 
             expr = new CreateTriggerStatement(cf, (name!=null?name.getText():null), (cls!=null?cls.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createTriggerStatement"


    // $ANTLR start "dropTriggerStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:568:1: dropTriggerStatement returns [DropTriggerStatement expr] : K_DROP K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName ;
    public final DropTriggerStatement dropTriggerStatement() throws RecognitionException {
        DropTriggerStatement expr = null;

        Token name=null;
        CFName cf = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:572:5: ( K_DROP K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:572:7: K_DROP K_TRIGGER (name= IDENT ) K_ON cf= columnFamilyName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropTriggerStatement2877); 
            match(input,K_TRIGGER,FOLLOW_K_TRIGGER_in_dropTriggerStatement2879); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:572:24: (name= IDENT )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:572:25: name= IDENT
            {
            name=(Token)match(input,IDENT,FOLLOW_IDENT_in_dropTriggerStatement2884); 

            }

            match(input,K_ON,FOLLOW_K_ON_in_dropTriggerStatement2887); 
            pushFollow(FOLLOW_columnFamilyName_in_dropTriggerStatement2891);
            cf=columnFamilyName();

            state._fsp--;

             expr = new DropTriggerStatement(cf, (name!=null?name.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "dropTriggerStatement"


    // $ANTLR start "alterKeyspaceStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:576:1: alterKeyspaceStatement returns [AlterKeyspaceStatement expr] : K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
    public final AlterKeyspaceStatement alterKeyspaceStatement() throws RecognitionException {
        AlterKeyspaceStatement expr = null;

        String ks = null;


         KSPropDefs attrs = new KSPropDefs(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:581:5: ( K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:581:7: K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterKeyspaceStatement2931); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement2933); 
            pushFollow(FOLLOW_keyspaceName_in_alterKeyspaceStatement2937);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_alterKeyspaceStatement2947); 
            pushFollow(FOLLOW_properties_in_alterKeyspaceStatement2949);
            properties(attrs);

            state._fsp--;

             expr = new AlterKeyspaceStatement(ks, attrs); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterKeyspaceStatement"


    // $ANTLR start "alterTableStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:586:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) ;
    public final AlterTableStatement alterTableStatement() throws RecognitionException {
        AlterTableStatement expr = null;

        CFName cf = null;

        ColumnIdentifier id = null;

        CQL3Type v = null;

        ColumnIdentifier id1 = null;

        ColumnIdentifier toId1 = null;

        ColumnIdentifier idn = null;

        ColumnIdentifier toIdn = null;



                AlterTableStatement.Type type = null;
                CFPropDefs props = new CFPropDefs();
                Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>();
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:599:5: ( K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:599:7: K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2985); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2987); 
            pushFollow(FOLLOW_columnFamilyName_in_alterTableStatement2991);
            cf=columnFamilyName();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:600:11: ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            int alt63=5;
            switch ( input.LA(1) ) {
            case K_ALTER:
                {
                alt63=1;
                }
                break;
            case K_ADD:
                {
                alt63=2;
                }
                break;
            case K_DROP:
                {
                alt63=3;
                }
                break;
            case K_WITH:
                {
                alt63=4;
                }
                break;
            case K_RENAME:
                {
                alt63=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 63, 0, input);

                throw nvae;
            }

            switch (alt63) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:600:13: K_ALTER id= cident K_TYPE v= comparatorType
                    {
                    match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement3005); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement3009);
                    id=cident();

                    state._fsp--;

                    match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement3011); 
                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement3015);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ALTER; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:601:13: K_ADD id= cident v= comparatorType
                    {
                    match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement3031); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement3037);
                    id=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement3041);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ADD; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:602:13: K_DROP id= cident
                    {
                    match(input,K_DROP,FOLLOW_K_DROP_in_alterTableStatement3064); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement3069);
                    id=cident();

                    state._fsp--;

                     type = AlterTableStatement.Type.DROP; 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:603:13: K_WITH properties[props]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_alterTableStatement3109); 
                    pushFollow(FOLLOW_properties_in_alterTableStatement3112);
                    properties(props);

                    state._fsp--;

                     type = AlterTableStatement.Type.OPTS; 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:604:13: K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )*
                    {
                    match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTableStatement3145); 
                     type = AlterTableStatement.Type.RENAME; 
                    pushFollow(FOLLOW_cident_in_alterTableStatement3199);
                    id1=cident();

                    state._fsp--;

                    match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement3201); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement3205);
                    toId1=cident();

                    state._fsp--;

                     renames.put(id1, toId1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:606:16: ( K_AND idn= cident K_TO toIdn= cident )*
                    loop62:
                    do {
                        int alt62=2;
                        int LA62_0 = input.LA(1);

                        if ( (LA62_0==K_AND) ) {
                            alt62=1;
                        }


                        switch (alt62) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:606:18: K_AND idn= cident K_TO toIdn= cident
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_alterTableStatement3226); 
                    	    pushFollow(FOLLOW_cident_in_alterTableStatement3230);
                    	    idn=cident();

                    	    state._fsp--;

                    	    match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement3232); 
                    	    pushFollow(FOLLOW_cident_in_alterTableStatement3236);
                    	    toIdn=cident();

                    	    state._fsp--;

                    	     renames.put(idn, toIdn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop62;
                        }
                    } while (true);


                    }
                    break;

            }


                    expr = new AlterTableStatement(cf, type, id, v, props, renames);
                

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterTableStatement"


    // $ANTLR start "alterTypeStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:613:1: alterTypeStatement returns [AlterTypeStatement expr] : K_ALTER K_TYPE name= non_type_ident ( K_ALTER f= cident K_TYPE v= comparatorType | K_ADD f= cident v= comparatorType | K_RENAME K_TO new_name= non_type_ident | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) ;
    public final AlterTypeStatement alterTypeStatement() throws RecognitionException {
        AlterTypeStatement expr = null;

        ColumnIdentifier name = null;

        ColumnIdentifier f = null;

        CQL3Type v = null;

        ColumnIdentifier new_name = null;

        ColumnIdentifier id1 = null;

        ColumnIdentifier toId1 = null;

        ColumnIdentifier idn = null;

        ColumnIdentifier toIdn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:619:5: ( K_ALTER K_TYPE name= non_type_ident ( K_ALTER f= cident K_TYPE v= comparatorType | K_ADD f= cident v= comparatorType | K_RENAME K_TO new_name= non_type_ident | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:619:7: K_ALTER K_TYPE name= non_type_ident ( K_ALTER f= cident K_TYPE v= comparatorType | K_ADD f= cident v= comparatorType | K_RENAME K_TO new_name= non_type_ident | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTypeStatement3282); 
            match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTypeStatement3284); 
            pushFollow(FOLLOW_non_type_ident_in_alterTypeStatement3288);
            name=non_type_ident();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:620:11: ( K_ALTER f= cident K_TYPE v= comparatorType | K_ADD f= cident v= comparatorType | K_RENAME K_TO new_name= non_type_ident | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            int alt65=4;
            switch ( input.LA(1) ) {
            case K_ALTER:
                {
                alt65=1;
                }
                break;
            case K_ADD:
                {
                alt65=2;
                }
                break;
            case K_RENAME:
                {
                int LA65_3 = input.LA(2);

                if ( (LA65_3==K_TO) ) {
                    alt65=3;
                }
                else if ( ((LA65_3>=K_DISTINCT && LA65_3<=K_AS)||(LA65_3>=K_FILTERING && LA65_3<=K_TTL)||LA65_3==K_VALUES||LA65_3==K_EXISTS||LA65_3==K_TIMESTAMP||LA65_3==K_COUNTER||(LA65_3>=K_KEY && LA65_3<=K_CUSTOM)||LA65_3==IDENT||LA65_3==K_TRIGGER||LA65_3==K_LIST||(LA65_3>=K_ALL && LA65_3<=QUOTED_NAME)||(LA65_3>=K_CONTAINS && LA65_3<=K_MAP)) ) {
                    alt65=4;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 65, 3, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 65, 0, input);

                throw nvae;
            }

            switch (alt65) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:620:13: K_ALTER f= cident K_TYPE v= comparatorType
                    {
                    match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTypeStatement3302); 
                    pushFollow(FOLLOW_cident_in_alterTypeStatement3306);
                    f=cident();

                    state._fsp--;

                    match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTypeStatement3308); 
                    pushFollow(FOLLOW_comparatorType_in_alterTypeStatement3312);
                    v=comparatorType();

                    state._fsp--;

                     expr = AlterTypeStatement.alter(name, f, v); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:621:13: K_ADD f= cident v= comparatorType
                    {
                    match(input,K_ADD,FOLLOW_K_ADD_in_alterTypeStatement3328); 
                    pushFollow(FOLLOW_cident_in_alterTypeStatement3334);
                    f=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_alterTypeStatement3338);
                    v=comparatorType();

                    state._fsp--;

                     expr = AlterTypeStatement.addition(name, f, v); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:622:13: K_RENAME K_TO new_name= non_type_ident
                    {
                    match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTypeStatement3361); 
                    match(input,K_TO,FOLLOW_K_TO_in_alterTypeStatement3363); 
                    pushFollow(FOLLOW_non_type_ident_in_alterTypeStatement3367);
                    new_name=non_type_ident();

                    state._fsp--;

                     expr = AlterTypeStatement.typeRename(name, new_name); 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:623:13: K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )*
                    {
                    match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTypeStatement3386); 
                     Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>(); 
                    pushFollow(FOLLOW_cident_in_alterTypeStatement3424);
                    id1=cident();

                    state._fsp--;

                    match(input,K_TO,FOLLOW_K_TO_in_alterTypeStatement3426); 
                    pushFollow(FOLLOW_cident_in_alterTypeStatement3430);
                    toId1=cident();

                    state._fsp--;

                     renames.put(id1, toId1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:626:18: ( K_AND idn= cident K_TO toIdn= cident )*
                    loop64:
                    do {
                        int alt64=2;
                        int LA64_0 = input.LA(1);

                        if ( (LA64_0==K_AND) ) {
                            alt64=1;
                        }


                        switch (alt64) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:626:20: K_AND idn= cident K_TO toIdn= cident
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_alterTypeStatement3453); 
                    	    pushFollow(FOLLOW_cident_in_alterTypeStatement3457);
                    	    idn=cident();

                    	    state._fsp--;

                    	    match(input,K_TO,FOLLOW_K_TO_in_alterTypeStatement3459); 
                    	    pushFollow(FOLLOW_cident_in_alterTypeStatement3463);
                    	    toIdn=cident();

                    	    state._fsp--;

                    	     renames.put(idn, toIdn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop64;
                        }
                    } while (true);

                     expr = AlterTypeStatement.renames(name, renames); 

                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterTypeStatement"


    // $ANTLR start "dropKeyspaceStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:632:1: dropKeyspaceStatement returns [DropKeyspaceStatement ksp] : K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName ;
    public final DropKeyspaceStatement dropKeyspaceStatement() throws RecognitionException {
        DropKeyspaceStatement ksp = null;

        String ks = null;


         boolean ifExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:637:5: ( K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:637:7: K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement3530); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement3532); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:637:25: ( K_IF K_EXISTS )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==K_IF) ) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:637:26: K_IF K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_dropKeyspaceStatement3535); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropKeyspaceStatement3537); 
                     ifExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_keyspaceName_in_dropKeyspaceStatement3546);
            ks=keyspaceName();

            state._fsp--;

             ksp = new DropKeyspaceStatement(ks, ifExists); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ksp;
    }
    // $ANTLR end "dropKeyspaceStatement"


    // $ANTLR start "dropTableStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:640:1: dropTableStatement returns [DropTableStatement stmt] : K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName ;
    public final DropTableStatement dropTableStatement() throws RecognitionException {
        DropTableStatement stmt = null;

        CFName cf = null;


         boolean ifExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:645:5: ( K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:645:7: K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropTableStatement3580); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropTableStatement3582); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:645:29: ( K_IF K_EXISTS )?
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==K_IF) ) {
                alt67=1;
            }
            switch (alt67) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:645:30: K_IF K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_dropTableStatement3585); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropTableStatement3587); 
                     ifExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_columnFamilyName_in_dropTableStatement3596);
            cf=columnFamilyName();

            state._fsp--;

             stmt = new DropTableStatement(cf, ifExists); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "dropTableStatement"


    // $ANTLR start "dropTypeStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:648:1: dropTypeStatement returns [DropTypeStatement stmt] : K_DROP K_TYPE ( K_IF K_EXISTS )? name= non_type_ident ;
    public final DropTypeStatement dropTypeStatement() throws RecognitionException {
        DropTypeStatement stmt = null;

        ColumnIdentifier name = null;


         boolean ifExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:653:5: ( K_DROP K_TYPE ( K_IF K_EXISTS )? name= non_type_ident )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:653:7: K_DROP K_TYPE ( K_IF K_EXISTS )? name= non_type_ident
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropTypeStatement3630); 
            match(input,K_TYPE,FOLLOW_K_TYPE_in_dropTypeStatement3632); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:653:21: ( K_IF K_EXISTS )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==K_IF) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:653:22: K_IF K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_dropTypeStatement3635); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropTypeStatement3637); 
                     ifExists = true; 

                    }
                    break;

            }

            pushFollow(FOLLOW_non_type_ident_in_dropTypeStatement3646);
            name=non_type_ident();

            state._fsp--;

             stmt = new DropTypeStatement(name, ifExists); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "dropTypeStatement"


    // $ANTLR start "dropIndexStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:656:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX ( K_IF K_EXISTS )? index= IDENT ;
    public final DropIndexStatement dropIndexStatement() throws RecognitionException {
        DropIndexStatement expr = null;

        Token index=null;

         boolean ifExists = false; 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:661:5: ( K_DROP K_INDEX ( K_IF K_EXISTS )? index= IDENT )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:661:7: K_DROP K_INDEX ( K_IF K_EXISTS )? index= IDENT
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement3680); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement3682); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:661:22: ( K_IF K_EXISTS )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==K_IF) ) {
                alt69=1;
            }
            switch (alt69) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:661:23: K_IF K_EXISTS
                    {
                    match(input,K_IF,FOLLOW_K_IF_in_dropIndexStatement3685); 
                    match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropIndexStatement3687); 
                     ifExists = true; 

                    }
                    break;

            }

            index=(Token)match(input,IDENT,FOLLOW_IDENT_in_dropIndexStatement3696); 
             expr = new DropIndexStatement((index!=null?index.getText():null), ifExists); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "dropIndexStatement"


    // $ANTLR start "truncateStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:665:1: truncateStatement returns [TruncateStatement stmt] : K_TRUNCATE cf= columnFamilyName ;
    public final TruncateStatement truncateStatement() throws RecognitionException {
        TruncateStatement stmt = null;

        CFName cf = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:669:5: ( K_TRUNCATE cf= columnFamilyName )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:669:7: K_TRUNCATE cf= columnFamilyName
            {
            match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement3727); 
            pushFollow(FOLLOW_columnFamilyName_in_truncateStatement3731);
            cf=columnFamilyName();

            state._fsp--;

             stmt = new TruncateStatement(cf); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "truncateStatement"


    // $ANTLR start "grantStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:672:1: grantStatement returns [GrantStatement stmt] : K_GRANT permissionOrAll K_ON resource K_TO username ;
    public final GrantStatement grantStatement() throws RecognitionException {
        GrantStatement stmt = null;

        Set<Permission> permissionOrAll1 = null;

        IResource resource2 = null;

        CqlParser.username_return username3 = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:676:5: ( K_GRANT permissionOrAll K_ON resource K_TO username )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:676:7: K_GRANT permissionOrAll K_ON resource K_TO username
            {
            match(input,K_GRANT,FOLLOW_K_GRANT_in_grantStatement3756); 
            pushFollow(FOLLOW_permissionOrAll_in_grantStatement3768);
            permissionOrAll1=permissionOrAll();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_grantStatement3776); 
            pushFollow(FOLLOW_resource_in_grantStatement3788);
            resource2=resource();

            state._fsp--;

            match(input,K_TO,FOLLOW_K_TO_in_grantStatement3796); 
            pushFollow(FOLLOW_username_in_grantStatement3808);
            username3=username();

            state._fsp--;

             stmt = new GrantStatement(permissionOrAll1, resource2, (username3!=null?input.toString(username3.start,username3.stop):null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "grantStatement"


    // $ANTLR start "revokeStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:685:1: revokeStatement returns [RevokeStatement stmt] : K_REVOKE permissionOrAll K_ON resource K_FROM username ;
    public final RevokeStatement revokeStatement() throws RecognitionException {
        RevokeStatement stmt = null;

        Set<Permission> permissionOrAll4 = null;

        IResource resource5 = null;

        CqlParser.username_return username6 = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:689:5: ( K_REVOKE permissionOrAll K_ON resource K_FROM username )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:689:7: K_REVOKE permissionOrAll K_ON resource K_FROM username
            {
            match(input,K_REVOKE,FOLLOW_K_REVOKE_in_revokeStatement3839); 
            pushFollow(FOLLOW_permissionOrAll_in_revokeStatement3851);
            permissionOrAll4=permissionOrAll();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_revokeStatement3859); 
            pushFollow(FOLLOW_resource_in_revokeStatement3871);
            resource5=resource();

            state._fsp--;

            match(input,K_FROM,FOLLOW_K_FROM_in_revokeStatement3879); 
            pushFollow(FOLLOW_username_in_revokeStatement3891);
            username6=username();

            state._fsp--;

             stmt = new RevokeStatement(permissionOrAll4, resource5, (username6!=null?input.toString(username6.start,username6.stop):null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "revokeStatement"


    // $ANTLR start "listPermissionsStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:698:1: listPermissionsStatement returns [ListPermissionsStatement stmt] : K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? ;
    public final ListPermissionsStatement listPermissionsStatement() throws RecognitionException {
        ListPermissionsStatement stmt = null;

        IResource resource7 = null;

        CqlParser.username_return username8 = null;

        Set<Permission> permissionOrAll9 = null;



                IResource resource = null;
                String username = null;
                boolean recursive = true;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:704:5: ( K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:704:7: K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )?
            {
            match(input,K_LIST,FOLLOW_K_LIST_in_listPermissionsStatement3929); 
            pushFollow(FOLLOW_permissionOrAll_in_listPermissionsStatement3941);
            permissionOrAll9=permissionOrAll();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:706:7: ( K_ON resource )?
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==K_ON) ) {
                alt70=1;
            }
            switch (alt70) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:706:9: K_ON resource
                    {
                    match(input,K_ON,FOLLOW_K_ON_in_listPermissionsStatement3951); 
                    pushFollow(FOLLOW_resource_in_listPermissionsStatement3953);
                    resource7=resource();

                    state._fsp--;

                     resource = resource7; 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:707:7: ( K_OF username )?
            int alt71=2;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==K_OF) ) {
                alt71=1;
            }
            switch (alt71) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:707:9: K_OF username
                    {
                    match(input,K_OF,FOLLOW_K_OF_in_listPermissionsStatement3968); 
                    pushFollow(FOLLOW_username_in_listPermissionsStatement3970);
                    username8=username();

                    state._fsp--;

                     username = (username8!=null?input.toString(username8.start,username8.stop):null); 

                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:708:7: ( K_NORECURSIVE )?
            int alt72=2;
            int LA72_0 = input.LA(1);

            if ( (LA72_0==K_NORECURSIVE) ) {
                alt72=1;
            }
            switch (alt72) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:708:9: K_NORECURSIVE
                    {
                    match(input,K_NORECURSIVE,FOLLOW_K_NORECURSIVE_in_listPermissionsStatement3985); 
                     recursive = false; 

                    }
                    break;

            }

             stmt = new ListPermissionsStatement(permissionOrAll9, resource, username, recursive); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "listPermissionsStatement"


    // $ANTLR start "permission"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:712:1: permission returns [Permission perm] : p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) ;
    public final Permission permission() throws RecognitionException {
        Permission perm = null;

        Token p=null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:713:5: (p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:713:7: p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE )
            {
            p=(Token)input.LT(1);
            if ( input.LA(1)==K_SELECT||input.LA(1)==K_CREATE||(input.LA(1)>=K_DROP && input.LA(1)<=K_ALTER)||(input.LA(1)>=K_MODIFY && input.LA(1)<=K_AUTHORIZE) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             perm = Permission.valueOf((p!=null?p.getText():null).toUpperCase()); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return perm;
    }
    // $ANTLR end "permission"


    // $ANTLR start "permissionOrAll"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:717:1: permissionOrAll returns [Set<Permission> perms] : ( K_ALL ( K_PERMISSIONS )? | p= permission ( K_PERMISSION )? );
    public final Set<Permission> permissionOrAll() throws RecognitionException {
        Set<Permission> perms = null;

        Permission p = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:718:5: ( K_ALL ( K_PERMISSIONS )? | p= permission ( K_PERMISSION )? )
            int alt75=2;
            int LA75_0 = input.LA(1);

            if ( (LA75_0==K_ALL) ) {
                alt75=1;
            }
            else if ( (LA75_0==K_SELECT||LA75_0==K_CREATE||(LA75_0>=K_DROP && LA75_0<=K_ALTER)||(LA75_0>=K_MODIFY && LA75_0<=K_AUTHORIZE)) ) {
                alt75=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 75, 0, input);

                throw nvae;
            }
            switch (alt75) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:718:7: K_ALL ( K_PERMISSIONS )?
                    {
                    match(input,K_ALL,FOLLOW_K_ALL_in_permissionOrAll4070); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:718:13: ( K_PERMISSIONS )?
                    int alt73=2;
                    int LA73_0 = input.LA(1);

                    if ( (LA73_0==K_PERMISSIONS) ) {
                        alt73=1;
                    }
                    switch (alt73) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:718:15: K_PERMISSIONS
                            {
                            match(input,K_PERMISSIONS,FOLLOW_K_PERMISSIONS_in_permissionOrAll4074); 

                            }
                            break;

                    }

                     perms = Permission.ALL_DATA; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:719:7: p= permission ( K_PERMISSION )?
                    {
                    pushFollow(FOLLOW_permission_in_permissionOrAll4095);
                    p=permission();

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:719:20: ( K_PERMISSION )?
                    int alt74=2;
                    int LA74_0 = input.LA(1);

                    if ( (LA74_0==K_PERMISSION) ) {
                        alt74=1;
                    }
                    switch (alt74) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:719:22: K_PERMISSION
                            {
                            match(input,K_PERMISSION,FOLLOW_K_PERMISSION_in_permissionOrAll4099); 

                            }
                            break;

                    }

                     perms = EnumSet.of(p); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return perms;
    }
    // $ANTLR end "permissionOrAll"


    // $ANTLR start "resource"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:722:1: resource returns [IResource res] : r= dataResource ;
    public final IResource resource() throws RecognitionException {
        IResource res = null;

        DataResource r = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:723:5: (r= dataResource )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:723:7: r= dataResource
            {
            pushFollow(FOLLOW_dataResource_in_resource4127);
            r=dataResource();

            state._fsp--;

             res = r; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return res;
    }
    // $ANTLR end "resource"


    // $ANTLR start "dataResource"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:726:1: dataResource returns [DataResource res] : ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName );
    public final DataResource dataResource() throws RecognitionException {
        DataResource res = null;

        String ks = null;

        CFName cf = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:727:5: ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName )
            int alt77=3;
            switch ( input.LA(1) ) {
            case K_ALL:
                {
                int LA77_1 = input.LA(2);

                if ( (LA77_1==K_KEYSPACES) ) {
                    alt77=1;
                }
                else if ( (LA77_1==EOF||LA77_1==K_FROM||LA77_1==K_TO||(LA77_1>=K_OF && LA77_1<=K_NORECURSIVE)||LA77_1==136||LA77_1==141) ) {
                    alt77=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 77, 1, input);

                    throw nvae;
                }
                }
                break;
            case K_KEYSPACE:
                {
                alt77=2;
                }
                break;
            case K_DISTINCT:
            case K_COUNT:
            case K_AS:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_COLUMNFAMILY:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case IDENT:
            case K_TRIGGER:
            case K_LIST:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case QUOTED_NAME:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt77=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 77, 0, input);

                throw nvae;
            }

            switch (alt77) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:727:7: K_ALL K_KEYSPACES
                    {
                    match(input,K_ALL,FOLLOW_K_ALL_in_dataResource4150); 
                    match(input,K_KEYSPACES,FOLLOW_K_KEYSPACES_in_dataResource4152); 
                     res = DataResource.root(); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:728:7: K_KEYSPACE ks= keyspaceName
                    {
                    match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dataResource4162); 
                    pushFollow(FOLLOW_keyspaceName_in_dataResource4168);
                    ks=keyspaceName();

                    state._fsp--;

                     res = DataResource.keyspace(ks); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:729:7: ( K_COLUMNFAMILY )? cf= columnFamilyName
                    {
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:729:7: ( K_COLUMNFAMILY )?
                    int alt76=2;
                    int LA76_0 = input.LA(1);

                    if ( (LA76_0==K_COLUMNFAMILY) ) {
                        alt76=1;
                    }
                    switch (alt76) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:729:9: K_COLUMNFAMILY
                            {
                            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dataResource4180); 

                            }
                            break;

                    }

                    pushFollow(FOLLOW_columnFamilyName_in_dataResource4189);
                    cf=columnFamilyName();

                    state._fsp--;

                     res = DataResource.columnFamily(cf.getKeyspace(), cf.getColumnFamily()); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return res;
    }
    // $ANTLR end "dataResource"


    // $ANTLR start "createUserStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:733:1: createUserStatement returns [CreateUserStatement stmt] : K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
    public final CreateUserStatement createUserStatement() throws RecognitionException {
        CreateUserStatement stmt = null;

        CqlParser.username_return username10 = null;



                UserOptions opts = new UserOptions();
                boolean superuser = false;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:741:5: ( K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:741:7: K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createUserStatement4229); 
            match(input,K_USER,FOLLOW_K_USER_in_createUserStatement4231); 
            pushFollow(FOLLOW_username_in_createUserStatement4233);
            username10=username();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:742:7: ( K_WITH userOptions[opts] )?
            int alt78=2;
            int LA78_0 = input.LA(1);

            if ( (LA78_0==K_WITH) ) {
                alt78=1;
            }
            switch (alt78) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:742:9: K_WITH userOptions[opts]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_createUserStatement4243); 
                    pushFollow(FOLLOW_userOptions_in_createUserStatement4245);
                    userOptions(opts);

                    state._fsp--;


                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:743:7: ( K_SUPERUSER | K_NOSUPERUSER )?
            int alt79=3;
            int LA79_0 = input.LA(1);

            if ( (LA79_0==K_SUPERUSER) ) {
                alt79=1;
            }
            else if ( (LA79_0==K_NOSUPERUSER) ) {
                alt79=2;
            }
            switch (alt79) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:743:9: K_SUPERUSER
                    {
                    match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_createUserStatement4259); 
                     superuser = true; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:743:45: K_NOSUPERUSER
                    {
                    match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_createUserStatement4265); 
                     superuser = false; 

                    }
                    break;

            }

             stmt = new CreateUserStatement((username10!=null?input.toString(username10.start,username10.stop):null), opts, superuser); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "createUserStatement"


    // $ANTLR start "alterUserStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:747:1: alterUserStatement returns [AlterUserStatement stmt] : K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
    public final AlterUserStatement alterUserStatement() throws RecognitionException {
        AlterUserStatement stmt = null;

        CqlParser.username_return username11 = null;



                UserOptions opts = new UserOptions();
                Boolean superuser = null;
            
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:755:5: ( K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:755:7: K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterUserStatement4310); 
            match(input,K_USER,FOLLOW_K_USER_in_alterUserStatement4312); 
            pushFollow(FOLLOW_username_in_alterUserStatement4314);
            username11=username();

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:756:7: ( K_WITH userOptions[opts] )?
            int alt80=2;
            int LA80_0 = input.LA(1);

            if ( (LA80_0==K_WITH) ) {
                alt80=1;
            }
            switch (alt80) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:756:9: K_WITH userOptions[opts]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_alterUserStatement4324); 
                    pushFollow(FOLLOW_userOptions_in_alterUserStatement4326);
                    userOptions(opts);

                    state._fsp--;


                    }
                    break;

            }

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:757:7: ( K_SUPERUSER | K_NOSUPERUSER )?
            int alt81=3;
            int LA81_0 = input.LA(1);

            if ( (LA81_0==K_SUPERUSER) ) {
                alt81=1;
            }
            else if ( (LA81_0==K_NOSUPERUSER) ) {
                alt81=2;
            }
            switch (alt81) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:757:9: K_SUPERUSER
                    {
                    match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_alterUserStatement4340); 
                     superuser = true; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:757:45: K_NOSUPERUSER
                    {
                    match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_alterUserStatement4346); 
                     superuser = false; 

                    }
                    break;

            }

             stmt = new AlterUserStatement((username11!=null?input.toString(username11.start,username11.stop):null), opts, superuser); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "alterUserStatement"


    // $ANTLR start "dropUserStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:761:1: dropUserStatement returns [DropUserStatement stmt] : K_DROP K_USER username ;
    public final DropUserStatement dropUserStatement() throws RecognitionException {
        DropUserStatement stmt = null;

        CqlParser.username_return username12 = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:765:5: ( K_DROP K_USER username )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:765:7: K_DROP K_USER username
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropUserStatement4382); 
            match(input,K_USER,FOLLOW_K_USER_in_dropUserStatement4384); 
            pushFollow(FOLLOW_username_in_dropUserStatement4386);
            username12=username();

            state._fsp--;

             stmt = new DropUserStatement((username12!=null?input.toString(username12.start,username12.stop):null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "dropUserStatement"


    // $ANTLR start "listUsersStatement"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:768:1: listUsersStatement returns [ListUsersStatement stmt] : K_LIST K_USERS ;
    public final ListUsersStatement listUsersStatement() throws RecognitionException {
        ListUsersStatement stmt = null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:772:5: ( K_LIST K_USERS )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:772:7: K_LIST K_USERS
            {
            match(input,K_LIST,FOLLOW_K_LIST_in_listUsersStatement4411); 
            match(input,K_USERS,FOLLOW_K_USERS_in_listUsersStatement4413); 
             stmt = new ListUsersStatement(); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "listUsersStatement"


    // $ANTLR start "userOptions"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:775:1: userOptions[UserOptions opts] : userOption[opts] ;
    public final void userOptions(UserOptions opts) throws RecognitionException {
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:776:5: ( userOption[opts] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:776:7: userOption[opts]
            {
            pushFollow(FOLLOW_userOption_in_userOptions4433);
            userOption(opts);

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "userOptions"


    // $ANTLR start "userOption"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:779:1: userOption[UserOptions opts] : k= K_PASSWORD v= STRING_LITERAL ;
    public final void userOption(UserOptions opts) throws RecognitionException {
        Token k=null;
        Token v=null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:780:5: (k= K_PASSWORD v= STRING_LITERAL )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:780:7: k= K_PASSWORD v= STRING_LITERAL
            {
            k=(Token)match(input,K_PASSWORD,FOLLOW_K_PASSWORD_in_userOption4454); 
            v=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_userOption4458); 
             opts.put((k!=null?k.getText():null), (v!=null?v.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "userOption"


    // $ANTLR start "cident"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:783:1: cident returns [ColumnIdentifier id] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final ColumnIdentifier cident() throws RecognitionException {
        ColumnIdentifier id = null;

        Token t=null;
        String k = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:787:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt82=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt82=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt82=2;
                }
                break;
            case K_DISTINCT:
            case K_COUNT:
            case K_AS:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt82=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 82, 0, input);

                throw nvae;
            }

            switch (alt82) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:787:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cident4487); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:788:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cident4512); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:789:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cident4531);
                    k=unreserved_keyword();

                    state._fsp--;

                     id = new ColumnIdentifier(k, false); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return id;
    }
    // $ANTLR end "cident"


    // $ANTLR start "keyspaceName"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:793:1: keyspaceName returns [String id] : cfOrKsName[name, true] ;
    public final String keyspaceName() throws RecognitionException {
        String id = null;

         CFName name = new CFName(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:795:5: ( cfOrKsName[name, true] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:795:7: cfOrKsName[name, true]
            {
            pushFollow(FOLLOW_cfOrKsName_in_keyspaceName4564);
            cfOrKsName(name, true);

            state._fsp--;

             id = name.getKeyspace(); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return id;
    }
    // $ANTLR end "keyspaceName"


    // $ANTLR start "columnFamilyName"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:798:1: columnFamilyName returns [CFName name] : ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] ;
    public final CFName columnFamilyName() throws RecognitionException {
        CFName name = null;

         name = new CFName(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:800:5: ( ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:800:7: ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false]
            {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:800:7: ( cfOrKsName[name, true] '.' )?
            int alt83=2;
            alt83 = dfa83.predict(input);
            switch (alt83) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:800:8: cfOrKsName[name, true] '.'
                    {
                    pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName4598);
                    cfOrKsName(name, true);

                    state._fsp--;

                    match(input,141,FOLLOW_141_in_columnFamilyName4601); 

                    }
                    break;

            }

            pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName4605);
            cfOrKsName(name, false);

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return name;
    }
    // $ANTLR end "columnFamilyName"


    // $ANTLR start "cfOrKsName"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:803:1: cfOrKsName[CFName name, boolean isKs] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final void cfOrKsName(CFName name, boolean isKs) throws RecognitionException {
        Token t=null;
        String k = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:804:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt84=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt84=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt84=2;
                }
                break;
            case K_DISTINCT:
            case K_COUNT:
            case K_AS:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt84=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 84, 0, input);

                throw nvae;
            }

            switch (alt84) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:804:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cfOrKsName4626); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), false); else name.setColumnFamily((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:805:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cfOrKsName4651); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), true); else name.setColumnFamily((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:806:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cfOrKsName4670);
                    k=unreserved_keyword();

                    state._fsp--;

                     if (isKs) name.setKeyspace(k, false); else name.setColumnFamily(k, false); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfOrKsName"


    // $ANTLR start "constant"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:809:1: constant returns [Constants.Literal constant] : (t= STRING_LITERAL | t= INTEGER | t= FLOAT | t= BOOLEAN | t= UUID | t= HEXNUMBER | ( '-' )? t= ( K_NAN | K_INFINITY ) );
    public final Constants.Literal constant() throws RecognitionException {
        Constants.Literal constant = null;

        Token t=null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:810:5: (t= STRING_LITERAL | t= INTEGER | t= FLOAT | t= BOOLEAN | t= UUID | t= HEXNUMBER | ( '-' )? t= ( K_NAN | K_INFINITY ) )
            int alt86=7;
            switch ( input.LA(1) ) {
            case STRING_LITERAL:
                {
                alt86=1;
                }
                break;
            case INTEGER:
                {
                alt86=2;
                }
                break;
            case FLOAT:
                {
                alt86=3;
                }
                break;
            case BOOLEAN:
                {
                alt86=4;
                }
                break;
            case UUID:
                {
                alt86=5;
                }
                break;
            case HEXNUMBER:
                {
                alt86=6;
                }
                break;
            case K_NAN:
            case K_INFINITY:
            case 144:
                {
                alt86=7;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 86, 0, input);

                throw nvae;
            }

            switch (alt86) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:810:7: t= STRING_LITERAL
                    {
                    t=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_constant4695); 
                     constant = Constants.Literal.string((t!=null?t.getText():null)); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:811:7: t= INTEGER
                    {
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_constant4707); 
                     constant = Constants.Literal.integer((t!=null?t.getText():null)); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:812:7: t= FLOAT
                    {
                    t=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_constant4726); 
                     constant = Constants.Literal.floatingPoint((t!=null?t.getText():null)); 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:813:7: t= BOOLEAN
                    {
                    t=(Token)match(input,BOOLEAN,FOLLOW_BOOLEAN_in_constant4747); 
                     constant = Constants.Literal.bool((t!=null?t.getText():null)); 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:814:7: t= UUID
                    {
                    t=(Token)match(input,UUID,FOLLOW_UUID_in_constant4766); 
                     constant = Constants.Literal.uuid((t!=null?t.getText():null)); 

                    }
                    break;
                case 6 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:815:7: t= HEXNUMBER
                    {
                    t=(Token)match(input,HEXNUMBER,FOLLOW_HEXNUMBER_in_constant4788); 
                     constant = Constants.Literal.hex((t!=null?t.getText():null)); 

                    }
                    break;
                case 7 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:816:7: ( '-' )? t= ( K_NAN | K_INFINITY )
                    {
                     String sign=""; 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:816:27: ( '-' )?
                    int alt85=2;
                    int LA85_0 = input.LA(1);

                    if ( (LA85_0==144) ) {
                        alt85=1;
                    }
                    switch (alt85) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:816:28: '-'
                            {
                            match(input,144,FOLLOW_144_in_constant4806); 
                            sign = "-"; 

                            }
                            break;

                    }

                    t=(Token)input.LT(1);
                    if ( (input.LA(1)>=K_NAN && input.LA(1)<=K_INFINITY) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     constant = Constants.Literal.floatingPoint(sign + (t!=null?t.getText():null)); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return constant;
    }
    // $ANTLR end "constant"


    // $ANTLR start "map_literal"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:819:1: map_literal returns [Maps.Literal map] : '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}' ;
    public final Maps.Literal map_literal() throws RecognitionException {
        Maps.Literal map = null;

        Term.Raw k1 = null;

        Term.Raw v1 = null;

        Term.Raw kn = null;

        Term.Raw vn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:820:5: ( '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}' )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:820:7: '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}'
            {
            match(input,145,FOLLOW_145_in_map_literal4844); 
             List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:821:11: (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )?
            int alt88=2;
            int LA88_0 = input.LA(1);

            if ( (LA88_0==K_DISTINCT||LA88_0==K_AS||LA88_0==K_FILTERING||LA88_0==INTEGER||LA88_0==K_VALUES||LA88_0==K_EXISTS||LA88_0==K_TIMESTAMP||LA88_0==K_COUNTER||(LA88_0>=K_KEY && LA88_0<=K_CUSTOM)||LA88_0==IDENT||(LA88_0>=STRING_LITERAL && LA88_0<=K_TRIGGER)||LA88_0==K_LIST||(LA88_0>=K_ALL && LA88_0<=K_PASSWORD)||(LA88_0>=FLOAT && LA88_0<=K_TOKEN)||(LA88_0>=K_CONTAINS && LA88_0<=K_MAP)||LA88_0==137||LA88_0==142||(LA88_0>=144 && LA88_0<=146)) ) {
                alt88=1;
            }
            switch (alt88) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:821:13: k1= term ':' v1= term ( ',' kn= term ':' vn= term )*
                    {
                    pushFollow(FOLLOW_term_in_map_literal4862);
                    k1=term();

                    state._fsp--;

                    match(input,146,FOLLOW_146_in_map_literal4864); 
                    pushFollow(FOLLOW_term_in_map_literal4868);
                    v1=term();

                    state._fsp--;

                     m.add(Pair.create(k1, v1)); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:821:65: ( ',' kn= term ':' vn= term )*
                    loop87:
                    do {
                        int alt87=2;
                        int LA87_0 = input.LA(1);

                        if ( (LA87_0==139) ) {
                            alt87=1;
                        }


                        switch (alt87) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:821:67: ',' kn= term ':' vn= term
                    	    {
                    	    match(input,139,FOLLOW_139_in_map_literal4874); 
                    	    pushFollow(FOLLOW_term_in_map_literal4878);
                    	    kn=term();

                    	    state._fsp--;

                    	    match(input,146,FOLLOW_146_in_map_literal4880); 
                    	    pushFollow(FOLLOW_term_in_map_literal4884);
                    	    vn=term();

                    	    state._fsp--;

                    	     m.add(Pair.create(kn, vn)); 

                    	    }
                    	    break;

                    	default :
                    	    break loop87;
                        }
                    } while (true);


                    }
                    break;

            }

            match(input,147,FOLLOW_147_in_map_literal4900); 
             map = new Maps.Literal(m); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return map;
    }
    // $ANTLR end "map_literal"


    // $ANTLR start "set_or_map"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:825:1: set_or_map[Term.Raw t] returns [Term.Raw value] : ( ':' v= term ( ',' kn= term ':' vn= term )* | ( ',' tn= term )* );
    public final Term.Raw set_or_map(Term.Raw t) throws RecognitionException {
        Term.Raw value = null;

        Term.Raw v = null;

        Term.Raw kn = null;

        Term.Raw vn = null;

        Term.Raw tn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:826:5: ( ':' v= term ( ',' kn= term ':' vn= term )* | ( ',' tn= term )* )
            int alt91=2;
            int LA91_0 = input.LA(1);

            if ( (LA91_0==146) ) {
                alt91=1;
            }
            else if ( (LA91_0==139||LA91_0==147) ) {
                alt91=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 91, 0, input);

                throw nvae;
            }
            switch (alt91) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:826:7: ':' v= term ( ',' kn= term ':' vn= term )*
                    {
                    match(input,146,FOLLOW_146_in_set_or_map4924); 
                    pushFollow(FOLLOW_term_in_set_or_map4928);
                    v=term();

                    state._fsp--;

                     List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); m.add(Pair.create(t, v)); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:827:11: ( ',' kn= term ':' vn= term )*
                    loop89:
                    do {
                        int alt89=2;
                        int LA89_0 = input.LA(1);

                        if ( (LA89_0==139) ) {
                            alt89=1;
                        }


                        switch (alt89) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:827:13: ',' kn= term ':' vn= term
                    	    {
                    	    match(input,139,FOLLOW_139_in_set_or_map4944); 
                    	    pushFollow(FOLLOW_term_in_set_or_map4948);
                    	    kn=term();

                    	    state._fsp--;

                    	    match(input,146,FOLLOW_146_in_set_or_map4950); 
                    	    pushFollow(FOLLOW_term_in_set_or_map4954);
                    	    vn=term();

                    	    state._fsp--;

                    	     m.add(Pair.create(kn, vn)); 

                    	    }
                    	    break;

                    	default :
                    	    break loop89;
                        }
                    } while (true);

                     value = new Maps.Literal(m); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:829:7: ( ',' tn= term )*
                    {
                     List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:830:11: ( ',' tn= term )*
                    loop90:
                    do {
                        int alt90=2;
                        int LA90_0 = input.LA(1);

                        if ( (LA90_0==139) ) {
                            alt90=1;
                        }


                        switch (alt90) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:830:13: ',' tn= term
                    	    {
                    	    match(input,139,FOLLOW_139_in_set_or_map4989); 
                    	    pushFollow(FOLLOW_term_in_set_or_map4993);
                    	    tn=term();

                    	    state._fsp--;

                    	     s.add(tn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop90;
                        }
                    } while (true);

                     value = new Sets.Literal(s); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return value;
    }
    // $ANTLR end "set_or_map"


    // $ANTLR start "collection_literal"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:834:1: collection_literal returns [Term.Raw value] : ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= set_or_map[t] '}' | '{' '}' );
    public final Term.Raw collection_literal() throws RecognitionException {
        Term.Raw value = null;

        Term.Raw t1 = null;

        Term.Raw tn = null;

        Term.Raw t = null;

        Term.Raw v = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:835:5: ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= set_or_map[t] '}' | '{' '}' )
            int alt94=3;
            int LA94_0 = input.LA(1);

            if ( (LA94_0==142) ) {
                alt94=1;
            }
            else if ( (LA94_0==145) ) {
                int LA94_2 = input.LA(2);

                if ( (LA94_2==147) ) {
                    alt94=3;
                }
                else if ( (LA94_2==K_DISTINCT||LA94_2==K_AS||LA94_2==K_FILTERING||LA94_2==INTEGER||LA94_2==K_VALUES||LA94_2==K_EXISTS||LA94_2==K_TIMESTAMP||LA94_2==K_COUNTER||(LA94_2>=K_KEY && LA94_2<=K_CUSTOM)||LA94_2==IDENT||(LA94_2>=STRING_LITERAL && LA94_2<=K_TRIGGER)||LA94_2==K_LIST||(LA94_2>=K_ALL && LA94_2<=K_PASSWORD)||(LA94_2>=FLOAT && LA94_2<=K_TOKEN)||(LA94_2>=K_CONTAINS && LA94_2<=K_MAP)||LA94_2==137||LA94_2==142||(LA94_2>=144 && LA94_2<=146)) ) {
                    alt94=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 94, 2, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 94, 0, input);

                throw nvae;
            }
            switch (alt94) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:835:7: '[' (t1= term ( ',' tn= term )* )? ']'
                    {
                    match(input,142,FOLLOW_142_in_collection_literal5027); 
                     List<Term.Raw> l = new ArrayList<Term.Raw>(); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:836:11: (t1= term ( ',' tn= term )* )?
                    int alt93=2;
                    int LA93_0 = input.LA(1);

                    if ( (LA93_0==K_DISTINCT||LA93_0==K_AS||LA93_0==K_FILTERING||LA93_0==INTEGER||LA93_0==K_VALUES||LA93_0==K_EXISTS||LA93_0==K_TIMESTAMP||LA93_0==K_COUNTER||(LA93_0>=K_KEY && LA93_0<=K_CUSTOM)||LA93_0==IDENT||(LA93_0>=STRING_LITERAL && LA93_0<=K_TRIGGER)||LA93_0==K_LIST||(LA93_0>=K_ALL && LA93_0<=K_PASSWORD)||(LA93_0>=FLOAT && LA93_0<=K_TOKEN)||(LA93_0>=K_CONTAINS && LA93_0<=K_MAP)||LA93_0==137||LA93_0==142||(LA93_0>=144 && LA93_0<=146)) ) {
                        alt93=1;
                    }
                    switch (alt93) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:836:13: t1= term ( ',' tn= term )*
                            {
                            pushFollow(FOLLOW_term_in_collection_literal5045);
                            t1=term();

                            state._fsp--;

                             l.add(t1); 
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:836:36: ( ',' tn= term )*
                            loop92:
                            do {
                                int alt92=2;
                                int LA92_0 = input.LA(1);

                                if ( (LA92_0==139) ) {
                                    alt92=1;
                                }


                                switch (alt92) {
                            	case 1 :
                            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:836:38: ',' tn= term
                            	    {
                            	    match(input,139,FOLLOW_139_in_collection_literal5051); 
                            	    pushFollow(FOLLOW_term_in_collection_literal5055);
                            	    tn=term();

                            	    state._fsp--;

                            	     l.add(tn); 

                            	    }
                            	    break;

                            	default :
                            	    break loop92;
                                }
                            } while (true);


                            }
                            break;

                    }

                    match(input,143,FOLLOW_143_in_collection_literal5071); 
                     value = new Lists.Literal(l); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:838:7: '{' t= term v= set_or_map[t] '}'
                    {
                    match(input,145,FOLLOW_145_in_collection_literal5081); 
                    pushFollow(FOLLOW_term_in_collection_literal5085);
                    t=term();

                    state._fsp--;

                    pushFollow(FOLLOW_set_or_map_in_collection_literal5089);
                    v=set_or_map(t);

                    state._fsp--;

                     value = v; 
                    match(input,147,FOLLOW_147_in_collection_literal5094); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:841:7: '{' '}'
                    {
                    match(input,145,FOLLOW_145_in_collection_literal5112); 
                    match(input,147,FOLLOW_147_in_collection_literal5114); 
                     value = new Sets.Literal(Collections.<Term.Raw>emptyList()); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return value;
    }
    // $ANTLR end "collection_literal"


    // $ANTLR start "usertype_literal"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:844:1: usertype_literal returns [UserTypes.Literal ut] : '{' k1= cident ':' v1= term ( ',' kn= cident ':' vn= term )* '}' ;
    public final UserTypes.Literal usertype_literal() throws RecognitionException {
        UserTypes.Literal ut = null;

        ColumnIdentifier k1 = null;

        Term.Raw v1 = null;

        ColumnIdentifier kn = null;

        Term.Raw vn = null;


         Map<ColumnIdentifier, Term.Raw> m = new HashMap<ColumnIdentifier, Term.Raw>(); 
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:848:5: ( '{' k1= cident ':' v1= term ( ',' kn= cident ':' vn= term )* '}' )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:848:7: '{' k1= cident ':' v1= term ( ',' kn= cident ':' vn= term )* '}'
            {
            match(input,145,FOLLOW_145_in_usertype_literal5158); 
            pushFollow(FOLLOW_cident_in_usertype_literal5162);
            k1=cident();

            state._fsp--;

            match(input,146,FOLLOW_146_in_usertype_literal5164); 
            pushFollow(FOLLOW_term_in_usertype_literal5168);
            v1=term();

            state._fsp--;

             m.put(k1, v1); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:848:52: ( ',' kn= cident ':' vn= term )*
            loop95:
            do {
                int alt95=2;
                int LA95_0 = input.LA(1);

                if ( (LA95_0==139) ) {
                    alt95=1;
                }


                switch (alt95) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:848:54: ',' kn= cident ':' vn= term
            	    {
            	    match(input,139,FOLLOW_139_in_usertype_literal5174); 
            	    pushFollow(FOLLOW_cident_in_usertype_literal5178);
            	    kn=cident();

            	    state._fsp--;

            	    match(input,146,FOLLOW_146_in_usertype_literal5180); 
            	    pushFollow(FOLLOW_term_in_usertype_literal5184);
            	    vn=term();

            	    state._fsp--;

            	     m.put(kn, vn); 

            	    }
            	    break;

            	default :
            	    break loop95;
                }
            } while (true);

            match(input,147,FOLLOW_147_in_usertype_literal5191); 

            }

             ut = new UserTypes.Literal(m); 
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ut;
    }
    // $ANTLR end "usertype_literal"


    // $ANTLR start "value"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:851:1: value returns [Term.Raw value] : (c= constant | l= collection_literal | u= usertype_literal | K_NULL | ':' id= cident | QMARK );
    public final Term.Raw value() throws RecognitionException {
        Term.Raw value = null;

        Constants.Literal c = null;

        Term.Raw l = null;

        UserTypes.Literal u = null;

        ColumnIdentifier id = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:852:5: (c= constant | l= collection_literal | u= usertype_literal | K_NULL | ':' id= cident | QMARK )
            int alt96=6;
            alt96 = dfa96.predict(input);
            switch (alt96) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:852:7: c= constant
                    {
                    pushFollow(FOLLOW_constant_in_value5214);
                    c=constant();

                    state._fsp--;

                     value = c; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:853:7: l= collection_literal
                    {
                    pushFollow(FOLLOW_collection_literal_in_value5236);
                    l=collection_literal();

                    state._fsp--;

                     value = l; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:854:7: u= usertype_literal
                    {
                    pushFollow(FOLLOW_usertype_literal_in_value5248);
                    u=usertype_literal();

                    state._fsp--;

                     value = u; 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:855:7: K_NULL
                    {
                    match(input,K_NULL,FOLLOW_K_NULL_in_value5260); 
                     value = Constants.NULL_LITERAL; 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:856:7: ':' id= cident
                    {
                    match(input,146,FOLLOW_146_in_value5284); 
                    pushFollow(FOLLOW_cident_in_value5288);
                    id=cident();

                    state._fsp--;

                     value = newBindVariables(id); 

                    }
                    break;
                case 6 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:857:7: QMARK
                    {
                    match(input,QMARK,FOLLOW_QMARK_in_value5305); 
                     value = newBindVariables(null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return value;
    }
    // $ANTLR end "value"


    // $ANTLR start "intValue"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:860:1: intValue returns [Term.Raw value] : ( | t= INTEGER | ':' id= cident | QMARK );
    public final Term.Raw intValue() throws RecognitionException {
        Term.Raw value = null;

        Token t=null;
        ColumnIdentifier id = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:861:5: ( | t= INTEGER | ':' id= cident | QMARK )
            int alt97=4;
            switch ( input.LA(1) ) {
            case EOF:
            case K_WHERE:
            case K_ALLOW:
            case K_AND:
            case K_INSERT:
            case K_UPDATE:
            case K_SET:
            case K_DELETE:
            case K_APPLY:
            case 136:
                {
                alt97=1;
                }
                break;
            case INTEGER:
                {
                alt97=2;
                }
                break;
            case 146:
                {
                alt97=3;
                }
                break;
            case QMARK:
                {
                alt97=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 97, 0, input);

                throw nvae;
            }

            switch (alt97) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:862:5: 
                    {
                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:862:7: t= INTEGER
                    {
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_intValue5351); 
                     value = Constants.Literal.integer((t!=null?t.getText():null)); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:863:7: ':' id= cident
                    {
                    match(input,146,FOLLOW_146_in_intValue5365); 
                    pushFollow(FOLLOW_cident_in_intValue5369);
                    id=cident();

                    state._fsp--;

                     value = newBindVariables(id); 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:864:7: QMARK
                    {
                    match(input,QMARK,FOLLOW_QMARK_in_intValue5379); 
                     value = newBindVariables(null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return value;
    }
    // $ANTLR end "intValue"


    // $ANTLR start "functionName"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:867:1: functionName returns [String s] : (f= IDENT | u= unreserved_function_keyword | K_TOKEN );
    public final String functionName() throws RecognitionException {
        String s = null;

        Token f=null;
        String u = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:868:5: (f= IDENT | u= unreserved_function_keyword | K_TOKEN )
            int alt98=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt98=1;
                }
                break;
            case K_DISTINCT:
            case K_AS:
            case K_FILTERING:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt98=2;
                }
                break;
            case K_TOKEN:
                {
                alt98=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 98, 0, input);

                throw nvae;
            }

            switch (alt98) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:868:7: f= IDENT
                    {
                    f=(Token)match(input,IDENT,FOLLOW_IDENT_in_functionName5412); 
                     s = (f!=null?f.getText():null); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:869:7: u= unreserved_function_keyword
                    {
                    pushFollow(FOLLOW_unreserved_function_keyword_in_functionName5446);
                    u=unreserved_function_keyword();

                    state._fsp--;

                     s = u; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:870:7: K_TOKEN
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_functionName5456); 
                     s = "token"; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return s;
    }
    // $ANTLR end "functionName"


    // $ANTLR start "functionArgs"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:873:1: functionArgs returns [List<Term.Raw> a] : ( '(' ')' | '(' t1= term ( ',' tn= term )* ')' );
    public final List<Term.Raw> functionArgs() throws RecognitionException {
        List<Term.Raw> a = null;

        Term.Raw t1 = null;

        Term.Raw tn = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:874:5: ( '(' ')' | '(' t1= term ( ',' tn= term )* ')' )
            int alt100=2;
            int LA100_0 = input.LA(1);

            if ( (LA100_0==137) ) {
                int LA100_1 = input.LA(2);

                if ( (LA100_1==138) ) {
                    alt100=1;
                }
                else if ( (LA100_1==K_DISTINCT||LA100_1==K_AS||LA100_1==K_FILTERING||LA100_1==INTEGER||LA100_1==K_VALUES||LA100_1==K_EXISTS||LA100_1==K_TIMESTAMP||LA100_1==K_COUNTER||(LA100_1>=K_KEY && LA100_1<=K_CUSTOM)||LA100_1==IDENT||(LA100_1>=STRING_LITERAL && LA100_1<=K_TRIGGER)||LA100_1==K_LIST||(LA100_1>=K_ALL && LA100_1<=K_PASSWORD)||(LA100_1>=FLOAT && LA100_1<=K_TOKEN)||(LA100_1>=K_CONTAINS && LA100_1<=K_MAP)||LA100_1==137||LA100_1==142||(LA100_1>=144 && LA100_1<=146)) ) {
                    alt100=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 100, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 100, 0, input);

                throw nvae;
            }
            switch (alt100) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:874:7: '(' ')'
                    {
                    match(input,137,FOLLOW_137_in_functionArgs5501); 
                    match(input,138,FOLLOW_138_in_functionArgs5503); 
                     a = Collections.emptyList(); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:875:7: '(' t1= term ( ',' tn= term )* ')'
                    {
                    match(input,137,FOLLOW_137_in_functionArgs5513); 
                    pushFollow(FOLLOW_term_in_functionArgs5517);
                    t1=term();

                    state._fsp--;

                     List<Term.Raw> args = new ArrayList<Term.Raw>(); args.add(t1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:876:11: ( ',' tn= term )*
                    loop99:
                    do {
                        int alt99=2;
                        int LA99_0 = input.LA(1);

                        if ( (LA99_0==139) ) {
                            alt99=1;
                        }


                        switch (alt99) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:876:13: ',' tn= term
                    	    {
                    	    match(input,139,FOLLOW_139_in_functionArgs5533); 
                    	    pushFollow(FOLLOW_term_in_functionArgs5537);
                    	    tn=term();

                    	    state._fsp--;

                    	     args.add(tn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop99;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_functionArgs5551); 
                     a = args; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return a;
    }
    // $ANTLR end "functionArgs"


    // $ANTLR start "term"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:880:1: term returns [Term.Raw term] : (v= value | f= functionName args= functionArgs | '(' c= comparatorType ')' t= term );
    public final Term.Raw term() throws RecognitionException {
        Term.Raw term = null;

        Term.Raw v = null;

        String f = null;

        List<Term.Raw> args = null;

        CQL3Type c = null;

        Term.Raw t = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:881:5: (v= value | f= functionName args= functionArgs | '(' c= comparatorType ')' t= term )
            int alt101=3;
            switch ( input.LA(1) ) {
            case INTEGER:
            case STRING_LITERAL:
            case FLOAT:
            case BOOLEAN:
            case UUID:
            case HEXNUMBER:
            case K_NAN:
            case K_INFINITY:
            case K_NULL:
            case QMARK:
            case 142:
            case 144:
            case 145:
            case 146:
                {
                alt101=1;
                }
                break;
            case K_DISTINCT:
            case K_AS:
            case K_FILTERING:
            case K_VALUES:
            case K_EXISTS:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case IDENT:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_TOKEN:
            case K_CONTAINS:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt101=2;
                }
                break;
            case 137:
                {
                alt101=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 101, 0, input);

                throw nvae;
            }

            switch (alt101) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:881:7: v= value
                    {
                    pushFollow(FOLLOW_value_in_term5576);
                    v=value();

                    state._fsp--;

                     term = v; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:882:7: f= functionName args= functionArgs
                    {
                    pushFollow(FOLLOW_functionName_in_term5613);
                    f=functionName();

                    state._fsp--;

                    pushFollow(FOLLOW_functionArgs_in_term5617);
                    args=functionArgs();

                    state._fsp--;

                     term = new FunctionCall.Raw(f, args); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:883:7: '(' c= comparatorType ')' t= term
                    {
                    match(input,137,FOLLOW_137_in_term5627); 
                    pushFollow(FOLLOW_comparatorType_in_term5631);
                    c=comparatorType();

                    state._fsp--;

                    match(input,138,FOLLOW_138_in_term5633); 
                    pushFollow(FOLLOW_term_in_term5637);
                    t=term();

                    state._fsp--;

                     term = new TypeCast(c, t); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return term;
    }
    // $ANTLR end "term"


    // $ANTLR start "columnOperation"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:886:1: columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations] : (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term );
    public final void columnOperation(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations) throws RecognitionException {
        Token sig=null;
        Token i=null;
        ColumnIdentifier key = null;

        Term.Raw t = null;

        ColumnIdentifier c = null;

        Term.Raw k = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:887:5: (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term )
            int alt103=4;
            alt103 = dfa103.predict(input);
            switch (alt103) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:887:7: key= cident '=' t= term ( '+' c= cident )?
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation5660);
                    key=cident();

                    state._fsp--;

                    match(input,148,FOLLOW_148_in_columnOperation5662); 
                    pushFollow(FOLLOW_term_in_columnOperation5666);
                    t=term();

                    state._fsp--;

                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:887:29: ( '+' c= cident )?
                    int alt102=2;
                    int LA102_0 = input.LA(1);

                    if ( (LA102_0==149) ) {
                        alt102=1;
                    }
                    switch (alt102) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:887:30: '+' c= cident
                            {
                            match(input,149,FOLLOW_149_in_columnOperation5669); 
                            pushFollow(FOLLOW_cident_in_columnOperation5673);
                            c=cident();

                            state._fsp--;


                            }
                            break;

                    }


                              if (c == null)
                              {
                                  addRawUpdate(operations, key, new Operation.SetValue(t));
                              }
                              else
                              {
                                  if (!key.equals(c))
                                      addRecognitionError("Only expressions of the form X = <value> + X are supported.");
                                  addRawUpdate(operations, key, new Operation.Prepend(t));
                              }
                          

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:900:7: key= cident '=' c= cident sig= ( '+' | '-' ) t= term
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation5694);
                    key=cident();

                    state._fsp--;

                    match(input,148,FOLLOW_148_in_columnOperation5696); 
                    pushFollow(FOLLOW_cident_in_columnOperation5700);
                    c=cident();

                    state._fsp--;

                    sig=(Token)input.LT(1);
                    if ( input.LA(1)==144||input.LA(1)==149 ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                    pushFollow(FOLLOW_term_in_columnOperation5714);
                    t=term();

                    state._fsp--;


                              if (!key.equals(c))
                                  addRecognitionError("Only expressions of the form X = X " + (sig!=null?sig.getText():null) + "<value> are supported.");
                              addRawUpdate(operations, key, (sig!=null?sig.getText():null).equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
                          

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:906:7: key= cident '=' c= cident i= INTEGER
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation5732);
                    key=cident();

                    state._fsp--;

                    match(input,148,FOLLOW_148_in_columnOperation5734); 
                    pushFollow(FOLLOW_cident_in_columnOperation5738);
                    c=cident();

                    state._fsp--;

                    i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_columnOperation5742); 

                              // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
                              if (!key.equals(c))
                                  // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
                                  addRecognitionError("Only expressions of the form X = X " + ((i!=null?i.getText():null).charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
                              addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer((i!=null?i.getText():null))));
                          

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:914:7: key= cident '[' k= term ']' '=' t= term
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation5760);
                    key=cident();

                    state._fsp--;

                    match(input,142,FOLLOW_142_in_columnOperation5762); 
                    pushFollow(FOLLOW_term_in_columnOperation5766);
                    k=term();

                    state._fsp--;

                    match(input,143,FOLLOW_143_in_columnOperation5768); 
                    match(input,148,FOLLOW_148_in_columnOperation5770); 
                    pushFollow(FOLLOW_term_in_columnOperation5774);
                    t=term();

                    state._fsp--;


                              addRawUpdate(operations, key, new Operation.SetElement(k, t));
                          

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "columnOperation"


    // $ANTLR start "properties"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:920:1: properties[PropertyDefinitions props] : property[props] ( K_AND property[props] )* ;
    public final void properties(PropertyDefinitions props) throws RecognitionException {
        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:921:5: ( property[props] ( K_AND property[props] )* )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:921:7: property[props] ( K_AND property[props] )*
            {
            pushFollow(FOLLOW_property_in_properties5800);
            property(props);

            state._fsp--;

            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:921:23: ( K_AND property[props] )*
            loop104:
            do {
                int alt104=2;
                int LA104_0 = input.LA(1);

                if ( (LA104_0==K_AND) ) {
                    alt104=1;
                }


                switch (alt104) {
            	case 1 :
            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:921:24: K_AND property[props]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_properties5804); 
            	    pushFollow(FOLLOW_property_in_properties5806);
            	    property(props);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop104;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "properties"


    // $ANTLR start "property"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:924:1: property[PropertyDefinitions props] : k= cident '=' (simple= propertyValue | map= map_literal ) ;
    public final void property(PropertyDefinitions props) throws RecognitionException {
        ColumnIdentifier k = null;

        String simple = null;

        Maps.Literal map = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:925:5: (k= cident '=' (simple= propertyValue | map= map_literal ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:925:7: k= cident '=' (simple= propertyValue | map= map_literal )
            {
            pushFollow(FOLLOW_cident_in_property5829);
            k=cident();

            state._fsp--;

            match(input,148,FOLLOW_148_in_property5831); 
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:925:20: (simple= propertyValue | map= map_literal )
            int alt105=2;
            int LA105_0 = input.LA(1);

            if ( ((LA105_0>=K_DISTINCT && LA105_0<=K_AS)||(LA105_0>=K_FILTERING && LA105_0<=INTEGER)||LA105_0==K_VALUES||LA105_0==K_EXISTS||LA105_0==K_TIMESTAMP||LA105_0==K_COUNTER||(LA105_0>=K_KEY && LA105_0<=K_CUSTOM)||(LA105_0>=STRING_LITERAL && LA105_0<=K_TRIGGER)||LA105_0==K_LIST||(LA105_0>=K_ALL && LA105_0<=K_PASSWORD)||(LA105_0>=FLOAT && LA105_0<=K_INFINITY)||(LA105_0>=K_CONTAINS && LA105_0<=K_MAP)||LA105_0==144) ) {
                alt105=1;
            }
            else if ( (LA105_0==145) ) {
                alt105=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 105, 0, input);

                throw nvae;
            }
            switch (alt105) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:925:21: simple= propertyValue
                    {
                    pushFollow(FOLLOW_propertyValue_in_property5836);
                    simple=propertyValue();

                    state._fsp--;

                     try { props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:926:24: map= map_literal
                    {
                    pushFollow(FOLLOW_map_literal_in_property5865);
                    map=map_literal();

                    state._fsp--;

                     try { props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "property"


    // $ANTLR start "propertyValue"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:929:1: propertyValue returns [String str] : (c= constant | u= unreserved_keyword );
    public final String propertyValue() throws RecognitionException {
        String str = null;

        Constants.Literal c = null;

        String u = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:930:5: (c= constant | u= unreserved_keyword )
            int alt106=2;
            int LA106_0 = input.LA(1);

            if ( (LA106_0==INTEGER||LA106_0==STRING_LITERAL||(LA106_0>=FLOAT && LA106_0<=K_INFINITY)||LA106_0==144) ) {
                alt106=1;
            }
            else if ( ((LA106_0>=K_DISTINCT && LA106_0<=K_AS)||(LA106_0>=K_FILTERING && LA106_0<=K_TTL)||LA106_0==K_VALUES||LA106_0==K_EXISTS||LA106_0==K_TIMESTAMP||LA106_0==K_COUNTER||(LA106_0>=K_KEY && LA106_0<=K_CUSTOM)||LA106_0==K_TRIGGER||LA106_0==K_LIST||(LA106_0>=K_ALL && LA106_0<=K_PASSWORD)||(LA106_0>=K_CONTAINS && LA106_0<=K_MAP)) ) {
                alt106=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 106, 0, input);

                throw nvae;
            }
            switch (alt106) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:930:7: c= constant
                    {
                    pushFollow(FOLLOW_constant_in_propertyValue5893);
                    c=constant();

                    state._fsp--;

                     str = c.getRawText(); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:931:7: u= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_propertyValue5915);
                    u=unreserved_keyword();

                    state._fsp--;

                     str = u; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "propertyValue"


    // $ANTLR start "relationType"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:934:1: relationType returns [Relation.Type op] : ( '=' | '<' | '<=' | '>' | '>=' );
    public final Relation.Type relationType() throws RecognitionException {
        Relation.Type op = null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:935:5: ( '=' | '<' | '<=' | '>' | '>=' )
            int alt107=5;
            switch ( input.LA(1) ) {
            case 148:
                {
                alt107=1;
                }
                break;
            case 150:
                {
                alt107=2;
                }
                break;
            case 151:
                {
                alt107=3;
                }
                break;
            case 152:
                {
                alt107=4;
                }
                break;
            case 153:
                {
                alt107=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 107, 0, input);

                throw nvae;
            }

            switch (alt107) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:935:7: '='
                    {
                    match(input,148,FOLLOW_148_in_relationType5938); 
                     op = Relation.Type.EQ; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:936:7: '<'
                    {
                    match(input,150,FOLLOW_150_in_relationType5949); 
                     op = Relation.Type.LT; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:937:7: '<='
                    {
                    match(input,151,FOLLOW_151_in_relationType5960); 
                     op = Relation.Type.LTE; 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:938:7: '>'
                    {
                    match(input,152,FOLLOW_152_in_relationType5970); 
                     op = Relation.Type.GT; 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:939:7: '>='
                    {
                    match(input,153,FOLLOW_153_in_relationType5981); 
                     op = Relation.Type.GTE; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return op;
    }
    // $ANTLR end "relationType"


    // $ANTLR start "relation"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:942:1: relation[List<Relation> clauses] : (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN ( QMARK | ':' mid= cident ) | name= cident K_IN '(' (f1= term ( ',' fN= term )* )? ')' | name= cident K_CONTAINS t= term | '(' relation[$clauses] ')' );
    public final void relation(List<Relation> clauses) throws RecognitionException {
        ColumnIdentifier name = null;

        Relation.Type type = null;

        Term.Raw t = null;

        ColumnIdentifier name1 = null;

        ColumnIdentifier namen = null;

        ColumnIdentifier mid = null;

        Term.Raw f1 = null;

        Term.Raw fN = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:943:5: (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN ( QMARK | ':' mid= cident ) | name= cident K_IN '(' (f1= term ( ',' fN= term )* )? ')' | name= cident K_CONTAINS t= term | '(' relation[$clauses] ')' )
            int alt112=6;
            alt112 = dfa112.predict(input);
            switch (alt112) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:943:7: name= cident type= relationType t= term
                    {
                    pushFollow(FOLLOW_cident_in_relation6003);
                    name=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_relationType_in_relation6007);
                    type=relationType();

                    state._fsp--;

                    pushFollow(FOLLOW_term_in_relation6011);
                    t=term();

                    state._fsp--;

                     clauses.add(new Relation(name, type, t)); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:944:7: K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_relation6021); 
                     List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); 
                    match(input,137,FOLLOW_137_in_relation6044); 
                    pushFollow(FOLLOW_cident_in_relation6048);
                    name1=cident();

                    state._fsp--;

                     l.add(name1); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:946:46: ( ',' namen= cident )*
                    loop108:
                    do {
                        int alt108=2;
                        int LA108_0 = input.LA(1);

                        if ( (LA108_0==139) ) {
                            alt108=1;
                        }


                        switch (alt108) {
                    	case 1 :
                    	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:946:48: ',' namen= cident
                    	    {
                    	    match(input,139,FOLLOW_139_in_relation6054); 
                    	    pushFollow(FOLLOW_cident_in_relation6058);
                    	    namen=cident();

                    	    state._fsp--;

                    	     l.add(namen); 

                    	    }
                    	    break;

                    	default :
                    	    break loop108;
                        }
                    } while (true);

                    match(input,138,FOLLOW_138_in_relation6064); 
                    pushFollow(FOLLOW_relationType_in_relation6076);
                    type=relationType();

                    state._fsp--;

                    pushFollow(FOLLOW_term_in_relation6080);
                    t=term();

                    state._fsp--;


                                for (ColumnIdentifier id : l)
                                    clauses.add(new Relation(id, type, t, true));
                            

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:952:7: name= cident K_IN ( QMARK | ':' mid= cident )
                    {
                    pushFollow(FOLLOW_cident_in_relation6100);
                    name=cident();

                    state._fsp--;

                    match(input,K_IN,FOLLOW_K_IN_in_relation6102); 
                     Term.Raw marker = null; 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:952:52: ( QMARK | ':' mid= cident )
                    int alt109=2;
                    int LA109_0 = input.LA(1);

                    if ( (LA109_0==QMARK) ) {
                        alt109=1;
                    }
                    else if ( (LA109_0==146) ) {
                        alt109=2;
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 109, 0, input);

                        throw nvae;
                    }
                    switch (alt109) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:952:53: QMARK
                            {
                            match(input,QMARK,FOLLOW_QMARK_in_relation6107); 
                             marker = newINBindVariables(null); 

                            }
                            break;
                        case 2 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:952:100: ':' mid= cident
                            {
                            match(input,146,FOLLOW_146_in_relation6113); 
                            pushFollow(FOLLOW_cident_in_relation6117);
                            mid=cident();

                            state._fsp--;

                             marker = newINBindVariables(mid); 

                            }
                            break;

                    }

                     clauses.add(new Relation(name, Relation.Type.IN, marker)); 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:954:7: name= cident K_IN '(' (f1= term ( ',' fN= term )* )? ')'
                    {
                    pushFollow(FOLLOW_cident_in_relation6140);
                    name=cident();

                    state._fsp--;

                    match(input,K_IN,FOLLOW_K_IN_in_relation6142); 
                     Relation rel = Relation.createInRelation(name); 
                    match(input,137,FOLLOW_137_in_relation6153); 
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:955:12: (f1= term ( ',' fN= term )* )?
                    int alt111=2;
                    int LA111_0 = input.LA(1);

                    if ( (LA111_0==K_DISTINCT||LA111_0==K_AS||LA111_0==K_FILTERING||LA111_0==INTEGER||LA111_0==K_VALUES||LA111_0==K_EXISTS||LA111_0==K_TIMESTAMP||LA111_0==K_COUNTER||(LA111_0>=K_KEY && LA111_0<=K_CUSTOM)||LA111_0==IDENT||(LA111_0>=STRING_LITERAL && LA111_0<=K_TRIGGER)||LA111_0==K_LIST||(LA111_0>=K_ALL && LA111_0<=K_PASSWORD)||(LA111_0>=FLOAT && LA111_0<=K_TOKEN)||(LA111_0>=K_CONTAINS && LA111_0<=K_MAP)||LA111_0==137||LA111_0==142||(LA111_0>=144 && LA111_0<=146)) ) {
                        alt111=1;
                    }
                    switch (alt111) {
                        case 1 :
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:955:14: f1= term ( ',' fN= term )*
                            {
                            pushFollow(FOLLOW_term_in_relation6159);
                            f1=term();

                            state._fsp--;

                             rel.addInValue(f1); 
                            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:955:46: ( ',' fN= term )*
                            loop110:
                            do {
                                int alt110=2;
                                int LA110_0 = input.LA(1);

                                if ( (LA110_0==139) ) {
                                    alt110=1;
                                }


                                switch (alt110) {
                            	case 1 :
                            	    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:955:47: ',' fN= term
                            	    {
                            	    match(input,139,FOLLOW_139_in_relation6164); 
                            	    pushFollow(FOLLOW_term_in_relation6168);
                            	    fN=term();

                            	    state._fsp--;

                            	     rel.addInValue(fN); 

                            	    }
                            	    break;

                            	default :
                            	    break loop110;
                                }
                            } while (true);


                            }
                            break;

                    }

                    match(input,138,FOLLOW_138_in_relation6178); 
                     clauses.add(rel); 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:956:7: name= cident K_CONTAINS t= term
                    {
                    pushFollow(FOLLOW_cident_in_relation6190);
                    name=cident();

                    state._fsp--;

                    match(input,K_CONTAINS,FOLLOW_K_CONTAINS_in_relation6192); 
                     Relation.Type rt = Relation.Type.CONTAINS; 
                    pushFollow(FOLLOW_term_in_relation6208);
                    t=term();

                    state._fsp--;

                     clauses.add(new Relation(name, rt, t)); 

                    }
                    break;
                case 6 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:958:7: '(' relation[$clauses] ')'
                    {
                    match(input,137,FOLLOW_137_in_relation6218); 
                    pushFollow(FOLLOW_relation_in_relation6220);
                    relation(clauses);

                    state._fsp--;

                    match(input,138,FOLLOW_138_in_relation6223); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "relation"


    // $ANTLR start "comparatorType"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:961:1: comparatorType returns [CQL3Type t] : (c= native_type | c= collection_type | id= non_type_ident | s= STRING_LITERAL );
    public final CQL3Type comparatorType() throws RecognitionException {
        CQL3Type t = null;

        Token s=null;
        CQL3Type c = null;

        ColumnIdentifier id = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:962:5: (c= native_type | c= collection_type | id= non_type_ident | s= STRING_LITERAL )
            int alt113=4;
            switch ( input.LA(1) ) {
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt113=1;
                }
                break;
            case K_MAP:
                {
                int LA113_2 = input.LA(2);

                if ( (LA113_2==150) ) {
                    alt113=2;
                }
                else if ( (LA113_2==EOF||LA113_2==K_PRIMARY||LA113_2==136||(LA113_2>=138 && LA113_2<=139)||LA113_2==152) ) {
                    alt113=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 113, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_LIST:
                {
                int LA113_3 = input.LA(2);

                if ( (LA113_3==150) ) {
                    alt113=2;
                }
                else if ( (LA113_3==EOF||LA113_3==K_PRIMARY||LA113_3==136||(LA113_3>=138 && LA113_3<=139)||LA113_3==152) ) {
                    alt113=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 113, 3, input);

                    throw nvae;
                }
                }
                break;
            case K_SET:
                {
                alt113=2;
                }
                break;
            case K_DISTINCT:
            case K_AS:
            case K_FILTERING:
            case K_VALUES:
            case K_EXISTS:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case IDENT:
            case K_TRIGGER:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case QUOTED_NAME:
            case K_CONTAINS:
                {
                alt113=3;
                }
                break;
            case STRING_LITERAL:
                {
                alt113=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 113, 0, input);

                throw nvae;
            }

            switch (alt113) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:962:7: c= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_comparatorType6246);
                    c=native_type();

                    state._fsp--;

                     t = c; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:963:7: c= collection_type
                    {
                    pushFollow(FOLLOW_collection_type_in_comparatorType6262);
                    c=collection_type();

                    state._fsp--;

                     t = c; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:964:7: id= non_type_ident
                    {
                    pushFollow(FOLLOW_non_type_ident_in_comparatorType6274);
                    id=non_type_ident();

                    state._fsp--;

                     try { t = CQL3Type.UserDefined.create(id); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); }

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:965:7: s= STRING_LITERAL
                    {
                    s=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_comparatorType6287); 

                            try {
                                t = new CQL3Type.Custom((s!=null?s.getText():null));
                            } catch (SyntaxException e) {
                                addRecognitionError("Cannot parse type " + (s!=null?s.getText():null) + ": " + e.getMessage());
                            } catch (ConfigurationException e) {
                                addRecognitionError("Error setting type " + (s!=null?s.getText():null) + ": " + e.getMessage());
                            }
                          

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return t;
    }
    // $ANTLR end "comparatorType"


    // $ANTLR start "native_type"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:977:1: native_type returns [CQL3Type t] : ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID );
    public final CQL3Type native_type() throws RecognitionException {
        CQL3Type t = null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:978:5: ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID )
            int alt114=16;
            switch ( input.LA(1) ) {
            case K_ASCII:
                {
                alt114=1;
                }
                break;
            case K_BIGINT:
                {
                alt114=2;
                }
                break;
            case K_BLOB:
                {
                alt114=3;
                }
                break;
            case K_BOOLEAN:
                {
                alt114=4;
                }
                break;
            case K_COUNTER:
                {
                alt114=5;
                }
                break;
            case K_DECIMAL:
                {
                alt114=6;
                }
                break;
            case K_DOUBLE:
                {
                alt114=7;
                }
                break;
            case K_FLOAT:
                {
                alt114=8;
                }
                break;
            case K_INET:
                {
                alt114=9;
                }
                break;
            case K_INT:
                {
                alt114=10;
                }
                break;
            case K_TEXT:
                {
                alt114=11;
                }
                break;
            case K_TIMESTAMP:
                {
                alt114=12;
                }
                break;
            case K_UUID:
                {
                alt114=13;
                }
                break;
            case K_VARCHAR:
                {
                alt114=14;
                }
                break;
            case K_VARINT:
                {
                alt114=15;
                }
                break;
            case K_TIMEUUID:
                {
                alt114=16;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 114, 0, input);

                throw nvae;
            }

            switch (alt114) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:978:7: K_ASCII
                    {
                    match(input,K_ASCII,FOLLOW_K_ASCII_in_native_type6316); 
                     t = CQL3Type.Native.ASCII; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:979:7: K_BIGINT
                    {
                    match(input,K_BIGINT,FOLLOW_K_BIGINT_in_native_type6330); 
                     t = CQL3Type.Native.BIGINT; 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:980:7: K_BLOB
                    {
                    match(input,K_BLOB,FOLLOW_K_BLOB_in_native_type6343); 
                     t = CQL3Type.Native.BLOB; 

                    }
                    break;
                case 4 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:981:7: K_BOOLEAN
                    {
                    match(input,K_BOOLEAN,FOLLOW_K_BOOLEAN_in_native_type6358); 
                     t = CQL3Type.Native.BOOLEAN; 

                    }
                    break;
                case 5 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:982:7: K_COUNTER
                    {
                    match(input,K_COUNTER,FOLLOW_K_COUNTER_in_native_type6370); 
                     t = CQL3Type.Native.COUNTER; 

                    }
                    break;
                case 6 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:983:7: K_DECIMAL
                    {
                    match(input,K_DECIMAL,FOLLOW_K_DECIMAL_in_native_type6382); 
                     t = CQL3Type.Native.DECIMAL; 

                    }
                    break;
                case 7 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:984:7: K_DOUBLE
                    {
                    match(input,K_DOUBLE,FOLLOW_K_DOUBLE_in_native_type6394); 
                     t = CQL3Type.Native.DOUBLE; 

                    }
                    break;
                case 8 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:985:7: K_FLOAT
                    {
                    match(input,K_FLOAT,FOLLOW_K_FLOAT_in_native_type6407); 
                     t = CQL3Type.Native.FLOAT; 

                    }
                    break;
                case 9 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:986:7: K_INET
                    {
                    match(input,K_INET,FOLLOW_K_INET_in_native_type6421); 
                     t = CQL3Type.Native.INET;

                    }
                    break;
                case 10 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:987:7: K_INT
                    {
                    match(input,K_INT,FOLLOW_K_INT_in_native_type6436); 
                     t = CQL3Type.Native.INT; 

                    }
                    break;
                case 11 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:988:7: K_TEXT
                    {
                    match(input,K_TEXT,FOLLOW_K_TEXT_in_native_type6452); 
                     t = CQL3Type.Native.TEXT; 

                    }
                    break;
                case 12 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:989:7: K_TIMESTAMP
                    {
                    match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_native_type6467); 
                     t = CQL3Type.Native.TIMESTAMP; 

                    }
                    break;
                case 13 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:990:7: K_UUID
                    {
                    match(input,K_UUID,FOLLOW_K_UUID_in_native_type6477); 
                     t = CQL3Type.Native.UUID; 

                    }
                    break;
                case 14 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:991:7: K_VARCHAR
                    {
                    match(input,K_VARCHAR,FOLLOW_K_VARCHAR_in_native_type6492); 
                     t = CQL3Type.Native.VARCHAR; 

                    }
                    break;
                case 15 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:992:7: K_VARINT
                    {
                    match(input,K_VARINT,FOLLOW_K_VARINT_in_native_type6504); 
                     t = CQL3Type.Native.VARINT; 

                    }
                    break;
                case 16 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:993:7: K_TIMEUUID
                    {
                    match(input,K_TIMEUUID,FOLLOW_K_TIMEUUID_in_native_type6517); 
                     t = CQL3Type.Native.TIMEUUID; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return t;
    }
    // $ANTLR end "native_type"


    // $ANTLR start "collection_type"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:996:1: collection_type returns [CQL3Type pt] : ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' );
    public final CQL3Type collection_type() throws RecognitionException {
        CQL3Type pt = null;

        CQL3Type t1 = null;

        CQL3Type t2 = null;

        CQL3Type t = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:997:5: ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' )
            int alt115=3;
            switch ( input.LA(1) ) {
            case K_MAP:
                {
                alt115=1;
                }
                break;
            case K_LIST:
                {
                alt115=2;
                }
                break;
            case K_SET:
                {
                alt115=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 115, 0, input);

                throw nvae;
            }

            switch (alt115) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:997:7: K_MAP '<' t1= comparatorType ',' t2= comparatorType '>'
                    {
                    match(input,K_MAP,FOLLOW_K_MAP_in_collection_type6541); 
                    match(input,150,FOLLOW_150_in_collection_type6544); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type6548);
                    t1=comparatorType();

                    state._fsp--;

                    match(input,139,FOLLOW_139_in_collection_type6550); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type6554);
                    t2=comparatorType();

                    state._fsp--;

                    match(input,152,FOLLOW_152_in_collection_type6556); 
                     try {
                                // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
                                if (t1 != null && t2 != null)
                                    pt = CQL3Type.Collection.map(t1, t2);
                              } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1003:7: K_LIST '<' t= comparatorType '>'
                    {
                    match(input,K_LIST,FOLLOW_K_LIST_in_collection_type6574); 
                    match(input,150,FOLLOW_150_in_collection_type6576); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type6580);
                    t=comparatorType();

                    state._fsp--;

                    match(input,152,FOLLOW_152_in_collection_type6582); 
                     try { if (t != null) pt = CQL3Type.Collection.list(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1005:7: K_SET '<' t= comparatorType '>'
                    {
                    match(input,K_SET,FOLLOW_K_SET_in_collection_type6600); 
                    match(input,150,FOLLOW_150_in_collection_type6603); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type6607);
                    t=comparatorType();

                    state._fsp--;

                    match(input,152,FOLLOW_152_in_collection_type6609); 
                     try { if (t != null) pt = CQL3Type.Collection.set(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return pt;
    }
    // $ANTLR end "collection_type"

    public static class username_return extends ParserRuleReturnScope {
    };

    // $ANTLR start "username"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1009:1: username : ( IDENT | STRING_LITERAL );
    public final CqlParser.username_return username() throws RecognitionException {
        CqlParser.username_return retval = new CqlParser.username_return();
        retval.start = input.LT(1);

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1010:5: ( IDENT | STRING_LITERAL )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:
            {
            if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "username"


    // $ANTLR start "non_type_ident"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1016:1: non_type_ident returns [ColumnIdentifier id] : (t= IDENT | t= QUOTED_NAME | k= basic_unreserved_keyword );
    public final ColumnIdentifier non_type_ident() throws RecognitionException {
        ColumnIdentifier id = null;

        Token t=null;
        String k = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1017:5: (t= IDENT | t= QUOTED_NAME | k= basic_unreserved_keyword )
            int alt116=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt116=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt116=2;
                }
                break;
            case K_DISTINCT:
            case K_AS:
            case K_FILTERING:
            case K_VALUES:
            case K_EXISTS:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_CUSTOM:
            case K_TRIGGER:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_CONTAINS:
            case K_MAP:
                {
                alt116=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 116, 0, input);

                throw nvae;
            }

            switch (alt116) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1017:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_non_type_ident6669); 
                     if (reservedTypeNames.contains((t!=null?t.getText():null))) addRecognitionError("Invalid (reserved) user type name " + (t!=null?t.getText():null)); id = new ColumnIdentifier((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1018:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_non_type_ident6700); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1019:7: k= basic_unreserved_keyword
                    {
                    pushFollow(FOLLOW_basic_unreserved_keyword_in_non_type_ident6725);
                    k=basic_unreserved_keyword();

                    state._fsp--;

                     id = new ColumnIdentifier(k, false); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return id;
    }
    // $ANTLR end "non_type_ident"


    // $ANTLR start "unreserved_keyword"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1022:1: unreserved_keyword returns [String str] : (u= unreserved_function_keyword | k= ( K_TTL | K_COUNT | K_WRITETIME ) );
    public final String unreserved_keyword() throws RecognitionException {
        String str = null;

        Token k=null;
        String u = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1023:5: (u= unreserved_function_keyword | k= ( K_TTL | K_COUNT | K_WRITETIME ) )
            int alt117=2;
            int LA117_0 = input.LA(1);

            if ( (LA117_0==K_DISTINCT||LA117_0==K_AS||LA117_0==K_FILTERING||LA117_0==K_VALUES||LA117_0==K_EXISTS||LA117_0==K_TIMESTAMP||LA117_0==K_COUNTER||(LA117_0>=K_KEY && LA117_0<=K_CUSTOM)||LA117_0==K_TRIGGER||LA117_0==K_LIST||(LA117_0>=K_ALL && LA117_0<=K_PASSWORD)||(LA117_0>=K_CONTAINS && LA117_0<=K_MAP)) ) {
                alt117=1;
            }
            else if ( (LA117_0==K_COUNT||(LA117_0>=K_WRITETIME && LA117_0<=K_TTL)) ) {
                alt117=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 117, 0, input);

                throw nvae;
            }
            switch (alt117) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1023:7: u= unreserved_function_keyword
                    {
                    pushFollow(FOLLOW_unreserved_function_keyword_in_unreserved_keyword6750);
                    u=unreserved_function_keyword();

                    state._fsp--;

                     str = u; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1024:7: k= ( K_TTL | K_COUNT | K_WRITETIME )
                    {
                    k=(Token)input.LT(1);
                    if ( input.LA(1)==K_COUNT||(input.LA(1)>=K_WRITETIME && input.LA(1)<=K_TTL) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     str = (k!=null?k.getText():null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "unreserved_keyword"


    // $ANTLR start "unreserved_function_keyword"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1027:1: unreserved_function_keyword returns [String str] : (u= basic_unreserved_keyword | t= native_type );
    public final String unreserved_function_keyword() throws RecognitionException {
        String str = null;

        String u = null;

        CQL3Type t = null;


        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1028:5: (u= basic_unreserved_keyword | t= native_type )
            int alt118=2;
            int LA118_0 = input.LA(1);

            if ( (LA118_0==K_DISTINCT||LA118_0==K_AS||LA118_0==K_FILTERING||LA118_0==K_VALUES||LA118_0==K_EXISTS||(LA118_0>=K_KEY && LA118_0<=K_CUSTOM)||LA118_0==K_TRIGGER||LA118_0==K_LIST||(LA118_0>=K_ALL && LA118_0<=K_PASSWORD)||LA118_0==K_CONTAINS||LA118_0==K_MAP) ) {
                alt118=1;
            }
            else if ( (LA118_0==K_TIMESTAMP||LA118_0==K_COUNTER||(LA118_0>=K_ASCII && LA118_0<=K_TIMEUUID)) ) {
                alt118=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 118, 0, input);

                throw nvae;
            }
            switch (alt118) {
                case 1 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1028:7: u= basic_unreserved_keyword
                    {
                    pushFollow(FOLLOW_basic_unreserved_keyword_in_unreserved_function_keyword6801);
                    u=basic_unreserved_keyword();

                    state._fsp--;

                     str = u; 

                    }
                    break;
                case 2 :
                    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1029:7: t= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_unreserved_function_keyword6813);
                    t=native_type();

                    state._fsp--;

                     str = t.toString(); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "unreserved_function_keyword"


    // $ANTLR start "basic_unreserved_keyword"
    // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1032:1: basic_unreserved_keyword returns [String str] : k= ( K_KEY | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS ) ;
    public final String basic_unreserved_keyword() throws RecognitionException {
        String str = null;

        Token k=null;

        try {
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1033:5: (k= ( K_KEY | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS ) )
            // E:\\cassandra\\git/src/java/org/apache/cassandra/cql3/Cql.g:1033:7: k= ( K_KEY | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS )
            {
            k=(Token)input.LT(1);
            if ( input.LA(1)==K_DISTINCT||input.LA(1)==K_AS||input.LA(1)==K_FILTERING||input.LA(1)==K_VALUES||input.LA(1)==K_EXISTS||(input.LA(1)>=K_KEY && input.LA(1)<=K_CUSTOM)||input.LA(1)==K_TRIGGER||input.LA(1)==K_LIST||(input.LA(1)>=K_ALL && input.LA(1)<=K_PASSWORD)||input.LA(1)==K_CONTAINS||input.LA(1)==K_MAP ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             str = (k!=null?k.getText():null); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "basic_unreserved_keyword"

    // Delegated rules


    protected DFA2 dfa2 = new DFA2(this);
    protected DFA14 dfa14 = new DFA14(this);
    protected DFA35 dfa35 = new DFA35(this);
    protected DFA83 dfa83 = new DFA83(this);
    protected DFA96 dfa96 = new DFA96(this);
    protected DFA103 dfa103 = new DFA103(this);
    protected DFA112 dfa112 = new DFA112(this);
    static final String DFA2_eotS =
        "\40\uffff";
    static final String DFA2_eofS =
        "\40\uffff";
    static final String DFA2_minS =
        "\1\4\7\uffff\3\47\2\uffff\1\5\22\uffff";
    static final String DFA2_maxS =
        "\1\76\7\uffff\3\107\2\uffff\1\112\22\uffff";
    static final String DFA2_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\3\uffff\1\20\1\21\1\uffff"+
        "\1\10\1\11\1\23\1\27\1\31\1\12\1\13\1\14\1\15\1\25\1\30\1\33\1\16"+
        "\1\17\1\24\1\32\1\26\1\22";
    static final String DFA2_specialS =
        "\40\uffff}>";
    static final String[] DFA2_transitionS = {
            "\1\6\1\1\20\uffff\1\2\7\uffff\1\3\1\uffff\1\5\1\4\4\uffff\1"+
            "\10\17\uffff\1\11\1\12\3\uffff\1\7\1\13\1\14\1\15",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\16\1\uffff\1\17\5\uffff\1\22\2\23\3\uffff\1\21\21\uffff"+
            "\1\20",
            "\1\24\1\uffff\1\25\5\uffff\1\31\1\uffff\1\26\3\uffff\1\30"+
            "\21\uffff\1\27",
            "\1\33\1\uffff\1\32\5\uffff\1\35\27\uffff\1\34",
            "",
            "",
            "\1\37\40\uffff\1\37\17\uffff\2\37\11\uffff\3\37\6\uffff\1"+
            "\36",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA2_eot = DFA.unpackEncodedString(DFA2_eotS);
    static final short[] DFA2_eof = DFA.unpackEncodedString(DFA2_eofS);
    static final char[] DFA2_min = DFA.unpackEncodedStringToUnsignedChars(DFA2_minS);
    static final char[] DFA2_max = DFA.unpackEncodedStringToUnsignedChars(DFA2_maxS);
    static final short[] DFA2_accept = DFA.unpackEncodedString(DFA2_acceptS);
    static final short[] DFA2_special = DFA.unpackEncodedString(DFA2_specialS);
    static final short[][] DFA2_transition;

    static {
        int numStates = DFA2_transitionS.length;
        DFA2_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA2_transition[i] = DFA.unpackEncodedString(DFA2_transitionS[i]);
        }
    }

    class DFA2 extends DFA {

        public DFA2(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 2;
            this.eot = DFA2_eot;
            this.eof = DFA2_eof;
            this.min = DFA2_min;
            this.max = DFA2_max;
            this.accept = DFA2_accept;
            this.special = DFA2_special;
            this.transition = DFA2_transition;
        }
        public String getDescription() {
            return "205:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createTableStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropTableStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement | st23= createTriggerStatement | st24= dropTriggerStatement | st25= createTypeStatement | st26= alterTypeStatement | st27= dropTypeStatement );";
        }
    }
    static final String DFA14_eotS =
        "\31\uffff";
    static final String DFA14_eofS =
        "\31\uffff";
    static final String DFA14_minS =
        "\1\6\1\10\1\uffff\23\10\3\uffff";
    static final String DFA14_maxS =
        "\1\146\1\u008d\1\uffff\23\u008d\3\uffff";
    static final String DFA14_acceptS =
        "\2\uffff\1\1\23\uffff\1\4\1\2\1\3";
    static final String DFA14_specialS =
        "\31\uffff}>";
    static final String[] DFA14_transitionS = {
            "\1\3\1\2\1\3\6\uffff\1\3\1\24\1\25\6\uffff\1\3\2\uffff\1\3"+
            "\1\uffff\1\17\5\uffff\1\10\7\uffff\6\3\1\uffff\1\1\2\uffff\1"+
            "\3\10\uffff\1\3\4\uffff\11\3\1\2\10\uffff\1\26\1\uffff\1\3\1"+
            "\4\1\5\1\6\1\7\1\11\1\12\1\13\1\14\1\15\1\16\1\20\1\21\1\22"+
            "\1\23\1\3",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\26\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\27\2\2\1\uffff\1\2",
            "\2\2\177\uffff\1\30\2\2\1\uffff\1\2",
            "",
            "",
            ""
    };

    static final short[] DFA14_eot = DFA.unpackEncodedString(DFA14_eotS);
    static final short[] DFA14_eof = DFA.unpackEncodedString(DFA14_eofS);
    static final char[] DFA14_min = DFA.unpackEncodedStringToUnsignedChars(DFA14_minS);
    static final char[] DFA14_max = DFA.unpackEncodedStringToUnsignedChars(DFA14_maxS);
    static final short[] DFA14_accept = DFA.unpackEncodedString(DFA14_acceptS);
    static final short[] DFA14_special = DFA.unpackEncodedString(DFA14_specialS);
    static final short[][] DFA14_transition;

    static {
        int numStates = DFA14_transitionS.length;
        DFA14_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA14_transition[i] = DFA.unpackEncodedString(DFA14_transitionS[i]);
        }
    }

    class DFA14 extends DFA {

        public DFA14(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 14;
            this.eot = DFA14_eot;
            this.eof = DFA14_eof;
            this.min = DFA14_min;
            this.max = DFA14_max;
            this.accept = DFA14_accept;
            this.special = DFA14_special;
            this.transition = DFA14_transition;
        }
        public String getDescription() {
            return "287:8: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs )";
        }
    }
    static final String DFA35_eotS =
        "\27\uffff";
    static final String DFA35_eofS =
        "\27\uffff";
    static final String DFA35_minS =
        "\1\6\24\11\2\uffff";
    static final String DFA35_maxS =
        "\1\146\24\u008e\2\uffff";
    static final String DFA35_acceptS =
        "\25\uffff\1\2\1\1";
    static final String DFA35_specialS =
        "\27\uffff}>";
    static final String[] DFA35_transitionS = {
            "\1\3\1\24\1\3\6\uffff\1\3\2\24\6\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\17\5\uffff\1\10\7\uffff\6\3\1\uffff\1\1\2\uffff\1\3\10\uffff"+
            "\1\3\4\uffff\11\3\1\2\12\uffff\1\3\1\4\1\5\1\6\1\7\1\11\1\12"+
            "\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "\1\26\u0081\uffff\1\26\2\uffff\1\25",
            "",
            ""
    };

    static final short[] DFA35_eot = DFA.unpackEncodedString(DFA35_eotS);
    static final short[] DFA35_eof = DFA.unpackEncodedString(DFA35_eofS);
    static final char[] DFA35_min = DFA.unpackEncodedStringToUnsignedChars(DFA35_minS);
    static final char[] DFA35_max = DFA.unpackEncodedStringToUnsignedChars(DFA35_maxS);
    static final short[] DFA35_accept = DFA.unpackEncodedString(DFA35_acceptS);
    static final short[] DFA35_special = DFA.unpackEncodedString(DFA35_specialS);
    static final short[][] DFA35_transition;

    static {
        int numStates = DFA35_transitionS.length;
        DFA35_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA35_transition[i] = DFA.unpackEncodedString(DFA35_transitionS[i]);
        }
    }

    class DFA35 extends DFA {

        public DFA35(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 35;
            this.eot = DFA35_eot;
            this.eof = DFA35_eof;
            this.min = DFA35_min;
            this.max = DFA35_max;
            this.accept = DFA35_accept;
            this.special = DFA35_special;
            this.transition = DFA35_transition;
        }
        public String getDescription() {
            return "418:1: deleteOp returns [Operation.RawDeletion op] : (c= cident | c= cident '[' t= term ']' );";
        }
    }
    static final String DFA83_eotS =
        "\27\uffff";
    static final String DFA83_eofS =
        "\1\uffff\24\26\2\uffff";
    static final String DFA83_minS =
        "\1\6\24\11\2\uffff";
    static final String DFA83_maxS =
        "\1\146\24\u008d\2\uffff";
    static final String DFA83_acceptS =
        "\25\uffff\1\1\1\2";
    static final String DFA83_specialS =
        "\27\uffff}>";
    static final String[] DFA83_transitionS = {
            "\1\3\1\24\1\3\6\uffff\1\3\2\24\6\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\17\5\uffff\1\10\7\uffff\6\3\1\uffff\1\1\2\uffff\1\3\10\uffff"+
            "\1\3\4\uffff\11\3\1\2\12\uffff\1\3\1\4\1\5\1\6\1\7\1\11\1\12"+
            "\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "\3\26\1\uffff\2\26\15\uffff\1\26\2\uffff\1\26\10\uffff\1\26"+
            "\15\uffff\5\26\4\uffff\2\26\107\uffff\2\26\3\uffff\1\25",
            "",
            ""
    };

    static final short[] DFA83_eot = DFA.unpackEncodedString(DFA83_eotS);
    static final short[] DFA83_eof = DFA.unpackEncodedString(DFA83_eofS);
    static final char[] DFA83_min = DFA.unpackEncodedStringToUnsignedChars(DFA83_minS);
    static final char[] DFA83_max = DFA.unpackEncodedStringToUnsignedChars(DFA83_maxS);
    static final short[] DFA83_accept = DFA.unpackEncodedString(DFA83_acceptS);
    static final short[] DFA83_special = DFA.unpackEncodedString(DFA83_specialS);
    static final short[][] DFA83_transition;

    static {
        int numStates = DFA83_transitionS.length;
        DFA83_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA83_transition[i] = DFA.unpackEncodedString(DFA83_transitionS[i]);
        }
    }

    class DFA83 extends DFA {

        public DFA83(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 83;
            this.eot = DFA83_eot;
            this.eof = DFA83_eof;
            this.min = DFA83_min;
            this.max = DFA83_max;
            this.accept = DFA83_accept;
            this.special = DFA83_special;
            this.transition = DFA83_transition;
        }
        public String getDescription() {
            return "800:7: ( cfOrKsName[name, true] '.' )?";
        }
    }
    static final String DFA96_eotS =
        "\32\uffff";
    static final String DFA96_eofS =
        "\32\uffff";
    static final String DFA96_minS =
        "\1\22\2\uffff\1\6\3\uffff\22\u0089\1\uffff";
    static final String DFA96_maxS =
        "\1\u0092\2\uffff\1\u0093\3\uffff\22\u0092\1\uffff";
    static final String DFA96_acceptS =
        "\1\uffff\1\1\1\2\1\uffff\1\4\1\5\1\6\22\uffff\1\3";
    static final String DFA96_specialS =
        "\32\uffff}>";
    static final String[] DFA96_transitionS = {
            "\1\1\41\uffff\1\1\30\uffff\6\1\1\4\1\6\71\uffff\1\2\1\uffff"+
            "\1\1\1\3\1\5",
            "",
            "",
            "\1\10\1\31\1\10\6\uffff\1\10\2\31\1\2\5\uffff\1\10\2\uffff"+
            "\1\10\1\uffff\1\24\5\uffff\1\15\7\uffff\6\10\1\uffff\1\7\1\uffff"+
            "\1\2\1\10\10\uffff\1\10\4\uffff\11\10\1\31\11\2\1\uffff\1\10"+
            "\1\11\1\12\1\13\1\14\1\16\1\17\1\20\1\21\1\22\1\23\1\25\1\26"+
            "\1\27\1\30\1\10\42\uffff\1\2\4\uffff\1\2\1\uffff\4\2",
            "",
            "",
            "",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            "\1\2\10\uffff\1\31",
            ""
    };

    static final short[] DFA96_eot = DFA.unpackEncodedString(DFA96_eotS);
    static final short[] DFA96_eof = DFA.unpackEncodedString(DFA96_eofS);
    static final char[] DFA96_min = DFA.unpackEncodedStringToUnsignedChars(DFA96_minS);
    static final char[] DFA96_max = DFA.unpackEncodedStringToUnsignedChars(DFA96_maxS);
    static final short[] DFA96_accept = DFA.unpackEncodedString(DFA96_acceptS);
    static final short[] DFA96_special = DFA.unpackEncodedString(DFA96_specialS);
    static final short[][] DFA96_transition;

    static {
        int numStates = DFA96_transitionS.length;
        DFA96_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA96_transition[i] = DFA.unpackEncodedString(DFA96_transitionS[i]);
        }
    }

    class DFA96 extends DFA {

        public DFA96(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 96;
            this.eot = DFA96_eot;
            this.eof = DFA96_eof;
            this.min = DFA96_min;
            this.max = DFA96_max;
            this.accept = DFA96_accept;
            this.special = DFA96_special;
            this.transition = DFA96_transition;
        }
        public String getDescription() {
            return "851:1: value returns [Term.Raw value] : (c= constant | l= collection_literal | u= usertype_literal | K_NULL | ':' id= cident | QMARK );";
        }
    }
    static final String DFA103_eotS =
        "\56\uffff";
    static final String DFA103_eofS =
        "\56\uffff";
    static final String DFA103_minS =
        "\1\6\24\u008e\1\6\2\uffff\24\22\2\uffff";
    static final String DFA103_maxS =
        "\1\146\24\u0094\1\u0092\2\uffff\24\u0095\2\uffff";
    static final String DFA103_acceptS =
        "\26\uffff\1\4\1\1\24\uffff\1\3\1\2";
    static final String DFA103_specialS =
        "\56\uffff}>";
    static final String[] DFA103_transitionS = {
            "\1\3\1\24\1\3\6\uffff\1\3\2\24\6\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\17\5\uffff\1\10\7\uffff\6\3\1\uffff\1\1\2\uffff\1\3\10\uffff"+
            "\1\3\4\uffff\11\3\1\2\12\uffff\1\3\1\4\1\5\1\6\1\7\1\11\1\12"+
            "\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\31\1\53\1\31\6\uffff\1\31\2\53\1\27\5\uffff\1\31\2\uffff"+
            "\1\31\1\uffff\1\45\5\uffff\1\36\7\uffff\6\31\1\uffff\1\30\1"+
            "\uffff\1\27\1\31\10\uffff\1\31\4\uffff\11\31\1\52\11\27\1\uffff"+
            "\1\31\1\32\1\33\1\34\1\35\1\37\1\40\1\41\1\42\1\43\1\44\1\46"+
            "\1\47\1\50\1\51\1\31\42\uffff\1\27\4\uffff\1\27\1\uffff\3\27",
            "",
            "",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\166\uffff\1\27\6\uffff\1\55\4\uffff\1\55",
            "\1\54\175\uffff\1\55\4\uffff\1\55",
            "\1\54\175\uffff\1\55\4\uffff\1\55",
            "",
            ""
    };

    static final short[] DFA103_eot = DFA.unpackEncodedString(DFA103_eotS);
    static final short[] DFA103_eof = DFA.unpackEncodedString(DFA103_eofS);
    static final char[] DFA103_min = DFA.unpackEncodedStringToUnsignedChars(DFA103_minS);
    static final char[] DFA103_max = DFA.unpackEncodedStringToUnsignedChars(DFA103_maxS);
    static final short[] DFA103_accept = DFA.unpackEncodedString(DFA103_acceptS);
    static final short[] DFA103_special = DFA.unpackEncodedString(DFA103_specialS);
    static final short[][] DFA103_transition;

    static {
        int numStates = DFA103_transitionS.length;
        DFA103_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA103_transition[i] = DFA.unpackEncodedString(DFA103_transitionS[i]);
        }
    }

    class DFA103 extends DFA {

        public DFA103(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 103;
            this.eot = DFA103_eot;
            this.eof = DFA103_eof;
            this.min = DFA103_min;
            this.max = DFA103_max;
            this.accept = DFA103_accept;
            this.special = DFA103_special;
            this.transition = DFA103_transition;
        }
        public String getDescription() {
            return "886:1: columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations] : (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term );";
        }
    }
    static final String DFA112_eotS =
        "\34\uffff";
    static final String DFA112_eofS =
        "\34\uffff";
    static final String DFA112_minS =
        "\1\6\24\126\2\uffff\1\124\4\uffff";
    static final String DFA112_maxS =
        "\1\u0089\24\u0099\2\uffff\1\u0092\4\uffff";
    static final String DFA112_acceptS =
        "\25\uffff\1\2\1\6\1\uffff\1\1\1\5\1\3\1\4";
    static final String DFA112_specialS =
        "\34\uffff}>";
    static final String[] DFA112_transitionS = {
            "\1\3\1\24\1\3\6\uffff\1\3\2\24\6\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\17\5\uffff\1\10\7\uffff\6\3\1\uffff\1\1\2\uffff\1\3\10\uffff"+
            "\1\3\4\uffff\11\3\1\2\10\uffff\1\25\1\uffff\1\3\1\4\1\5\1\6"+
            "\1\7\1\11\1\12\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3\42"+
            "\uffff\1\26",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "\1\27\1\31\74\uffff\1\30\1\uffff\4\30",
            "",
            "",
            "\1\32\64\uffff\1\33\10\uffff\1\32",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA112_eot = DFA.unpackEncodedString(DFA112_eotS);
    static final short[] DFA112_eof = DFA.unpackEncodedString(DFA112_eofS);
    static final char[] DFA112_min = DFA.unpackEncodedStringToUnsignedChars(DFA112_minS);
    static final char[] DFA112_max = DFA.unpackEncodedStringToUnsignedChars(DFA112_maxS);
    static final short[] DFA112_accept = DFA.unpackEncodedString(DFA112_acceptS);
    static final short[] DFA112_special = DFA.unpackEncodedString(DFA112_specialS);
    static final short[][] DFA112_transition;

    static {
        int numStates = DFA112_transitionS.length;
        DFA112_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA112_transition[i] = DFA.unpackEncodedString(DFA112_transitionS[i]);
        }
    }

    class DFA112 extends DFA {

        public DFA112(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 112;
            this.eot = DFA112_eot;
            this.eof = DFA112_eof;
            this.min = DFA112_min;
            this.max = DFA112_max;
            this.accept = DFA112_accept;
            this.special = DFA112_special;
            this.transition = DFA112_transition;
        }
        public String getDescription() {
            return "942:1: relation[List<Relation> clauses] : (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN ( QMARK | ':' mid= cident ) | name= cident K_IN '(' (f1= term ( ',' fN= term )* )? ')' | name= cident K_CONTAINS t= term | '(' relation[$clauses] ')' );";
        }
    }
 

    public static final BitSet FOLLOW_cqlStatement_in_query72 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_query75 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_EOF_in_query79 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectStatement_in_cqlStatement113 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_cqlStatement138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_cqlStatement163 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_batchStatement_in_cqlStatement188 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_cqlStatement214 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useStatement_in_cqlStatement239 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_truncateStatement_in_cqlStatement267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createKeyspaceStatement_in_cqlStatement290 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createTableStatement_in_cqlStatement307 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createIndexStatement_in_cqlStatement326 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropKeyspaceStatement_in_cqlStatement345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropTableStatement_in_cqlStatement363 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropIndexStatement_in_cqlStatement384 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterTableStatement_in_cqlStatement405 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterKeyspaceStatement_in_cqlStatement425 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantStatement_in_cqlStatement442 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeStatement_in_cqlStatement467 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listPermissionsStatement_in_cqlStatement491 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createUserStatement_in_cqlStatement506 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterUserStatement_in_cqlStatement526 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropUserStatement_in_cqlStatement547 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listUsersStatement_in_cqlStatement569 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createTriggerStatement_in_cqlStatement590 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropTriggerStatement_in_cqlStatement607 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createTypeStatement_in_cqlStatement626 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterTypeStatement_in_cqlStatement646 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropTypeStatement_in_cqlStatement667 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USE_in_useStatement702 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_keyspaceName_in_useStatement706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SELECT_in_selectStatement740 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000001000L});
    public static final BitSet FOLLOW_K_DISTINCT_in_selectStatement746 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000001000L});
    public static final BitSet FOLLOW_selectClause_in_selectStatement755 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_COUNT_in_selectStatement775 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_selectStatement777 = new BitSet(new long[]{0x0000000000040000L,0x0000000000000000L,0x0000000000001000L});
    public static final BitSet FOLLOW_selectCountClause_in_selectStatement781 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_selectStatement783 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_K_AS_in_selectStatement788 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_selectStatement792 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_FROM_in_selectStatement807 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_selectStatement811 = new BitSet(new long[]{0x0000000000006C02L});
    public static final BitSet FOLLOW_K_WHERE_in_selectStatement821 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_whereClause_in_selectStatement825 = new BitSet(new long[]{0x0000000000006802L});
    public static final BitSet FOLLOW_K_ORDER_in_selectStatement838 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_K_BY_in_selectStatement840 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement842 = new BitSet(new long[]{0x0000000000006002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_selectStatement847 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement849 = new BitSet(new long[]{0x0000000000006002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_K_LIMIT_in_selectStatement866 = new BitSet(new long[]{0x0000000000044000L,0x0000000000100000L,0x0000000000040000L});
    public static final BitSet FOLLOW_intValue_in_selectStatement870 = new BitSet(new long[]{0x0000000000004002L});
    public static final BitSet FOLLOW_K_ALLOW_in_selectStatement885 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_K_FILTERING_in_selectStatement887 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selector_in_selectClause924 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_selectClause929 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L});
    public static final BitSet FOLLOW_selector_in_selectClause933 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_140_in_selectClause945 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaliasedSelector_in_selector978 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_K_AS_in_selector981 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_selector985 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_unaliasedSelector1026 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_K_WRITETIME_in_unaliasedSelector1072 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_unaliasedSelector1074 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_unaliasedSelector1078 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_unaliasedSelector1080 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_K_TTL_in_unaliasedSelector1106 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_unaliasedSelector1114 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_unaliasedSelector1118 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_unaliasedSelector1120 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_functionName_in_unaliasedSelector1148 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_selectionFunctionArgs_in_unaliasedSelector1152 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_141_in_unaliasedSelector1167 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_unaliasedSelector1171 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_137_in_selectionFunctionArgs1199 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_selectionFunctionArgs1201 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_selectionFunctionArgs1211 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L});
    public static final BitSet FOLLOW_unaliasedSelector_in_selectionFunctionArgs1215 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_selectionFunctionArgs1231 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L});
    public static final BitSet FOLLOW_unaliasedSelector_in_selectionFunctionArgs1235 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_selectionFunctionArgs1248 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_140_in_selectCountClause1271 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_selectCountClause1293 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_relation_in_whereClause1329 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_whereClause1333 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_relation_in_whereClause1335 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_cident_in_orderByClause1366 = new BitSet(new long[]{0x0000000000300002L});
    public static final BitSet FOLLOW_K_ASC_in_orderByClause1371 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_orderByClause1375 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INSERT_in_insertStatement1413 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_K_INTO_in_insertStatement1415 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_insertStatement1419 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_insertStatement1431 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_insertStatement1435 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_insertStatement1442 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_insertStatement1446 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_insertStatement1453 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_K_VALUES_in_insertStatement1463 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_insertStatement1475 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_insertStatement1479 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_insertStatement1485 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_insertStatement1489 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_insertStatement1496 = new BitSet(new long[]{0x0000000012000002L});
    public static final BitSet FOLLOW_K_IF_in_insertStatement1509 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_NOT_in_insertStatement1511 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_insertStatement1513 = new BitSet(new long[]{0x0000000010000002L});
    public static final BitSet FOLLOW_usingClause_in_insertStatement1530 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USING_in_usingClause1560 = new BitSet(new long[]{0x0000000020020000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1562 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_usingClause1567 = new BitSet(new long[]{0x0000000020020000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1569 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseObjective1591 = new BitSet(new long[]{0x0000000000040000L,0x0000000000100000L,0x0000000000040000L});
    public static final BitSet FOLLOW_intValue_in_usingClauseObjective1595 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective1605 = new BitSet(new long[]{0x0000000000040000L,0x0000000000100000L,0x0000000000040000L});
    public static final BitSet FOLLOW_intValue_in_usingClauseObjective1609 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1643 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_updateStatement1647 = new BitSet(new long[]{0x0000000090000000L});
    public static final BitSet FOLLOW_usingClause_in_updateStatement1657 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_K_SET_in_updateStatement1669 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnOperation_in_updateStatement1671 = new BitSet(new long[]{0x0000000000000400L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_updateStatement1675 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnOperation_in_updateStatement1677 = new BitSet(new long[]{0x0000000000000400L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_K_WHERE_in_updateStatement1688 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_whereClause_in_updateStatement1692 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_K_IF_in_updateStatement1702 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_updateCondition_in_updateStatement1706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnOperation_in_updateCondition1747 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_updateCondition1752 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnOperation_in_updateCondition1754 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1790 = new BitSet(new long[]{0x4025F808290383C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_deleteSelection_in_deleteStatement1796 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_FROM_in_deleteStatement1809 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_deleteStatement1813 = new BitSet(new long[]{0x0000000010000400L});
    public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1823 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1835 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_whereClause_in_deleteStatement1839 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_K_IF_in_deleteStatement1849 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_updateCondition_in_deleteStatement1853 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteOp_in_deleteSelection1899 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_deleteSelection1914 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_deleteOp_in_deleteSelection1918 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_cident_in_deleteOp1945 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_deleteOp1972 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
    public static final BitSet FOLLOW_142_in_deleteOp1974 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_deleteOp1978 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
    public static final BitSet FOLLOW_143_in_deleteOp1980 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USING_in_usingClauseDelete2000 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDelete2002 = new BitSet(new long[]{0x0000000000040000L,0x0000000000100000L,0x0000000000040000L});
    public static final BitSet FOLLOW_intValue_in_usingClauseDelete2006 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BEGIN_in_batchStatement2040 = new BitSet(new long[]{0x0000001C00000000L});
    public static final BitSet FOLLOW_K_UNLOGGED_in_batchStatement2050 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COUNTER_in_batchStatement2056 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement2069 = new BitSet(new long[]{0x0000002150400000L});
    public static final BitSet FOLLOW_usingClause_in_batchStatement2073 = new BitSet(new long[]{0x0000002140400000L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement2093 = new BitSet(new long[]{0x0000002140400000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_batchStatement2095 = new BitSet(new long[]{0x0000002140400000L});
    public static final BitSet FOLLOW_K_APPLY_in_batchStatement2109 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement2111 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective2142 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective2155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective2168 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement2203 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement2205 = new BitSet(new long[]{0x4025F8082B0381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_K_IF_in_createKeyspaceStatement2208 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_NOT_in_createKeyspaceStatement2210 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_createKeyspaceStatement2212 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_keyspaceName_in_createKeyspaceStatement2221 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement2229 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_properties_in_createKeyspaceStatement2231 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createTableStatement2266 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createTableStatement2268 = new BitSet(new long[]{0x4025F8082B0381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_K_IF_in_createTableStatement2271 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_NOT_in_createTableStatement2273 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_createTableStatement2275 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_createTableStatement2290 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_cfamDefinition_in_createTableStatement2300 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_cfamDefinition2319 = new BitSet(new long[]{0x4025FC08290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition2321 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_cfamDefinition2326 = new BitSet(new long[]{0x4025FC08290381C0L,0x0000007FFF801FF8L,0x0000000000000C00L});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition2328 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_cfamDefinition2335 = new BitSet(new long[]{0x0000010000000002L});
    public static final BitSet FOLLOW_K_WITH_in_cfamDefinition2345 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2347 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_cfamDefinition2352 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2354 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_cident_in_cfamColumns2380 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_cfamColumns2384 = new BitSet(new long[]{0x0000040000000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2389 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns2391 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2403 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns2405 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_cfamColumns2407 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_pkDef_in_cfamColumns2409 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_cfamColumns2413 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_cfamColumns2417 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_cfamColumns2424 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_pkDef2444 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_pkDef2454 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_pkDef2460 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_pkDef2466 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_pkDef2470 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_pkDef2477 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_property_in_cfamProperty2497 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COMPACT_in_cfamProperty2506 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_K_STORAGE_in_cfamProperty2508 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CLUSTERING_in_cfamProperty2518 = new BitSet(new long[]{0x0000000000000800L});
    public static final BitSet FOLLOW_K_ORDER_in_cfamProperty2520 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_K_BY_in_cfamProperty2522 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_cfamProperty2524 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2526 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_cfamProperty2530 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2532 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_cfamProperty2537 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_cfamOrdering2565 = new BitSet(new long[]{0x0000000000300000L});
    public static final BitSet FOLLOW_K_ASC_in_cfamOrdering2568 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_cfamOrdering2572 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createTypeStatement2611 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_createTypeStatement2613 = new BitSet(new long[]{0x4025F8000B008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_K_IF_in_createTypeStatement2616 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_NOT_in_createTypeStatement2618 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_createTypeStatement2620 = new BitSet(new long[]{0x4025F80009008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_non_type_ident_in_createTypeStatement2638 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_createTypeStatement2651 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_typeColumns_in_createTypeStatement2653 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_createTypeStatement2658 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L,0x0000000000000C00L});
    public static final BitSet FOLLOW_typeColumns_in_createTypeStatement2660 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_createTypeStatement2667 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_typeColumns2687 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_typeColumns2691 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement2726 = new BitSet(new long[]{0x0003000000000000L});
    public static final BitSet FOLLOW_K_CUSTOM_in_createIndexStatement2729 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement2735 = new BitSet(new long[]{0x000C000002000000L});
    public static final BitSet FOLLOW_K_IF_in_createIndexStatement2738 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_NOT_in_createIndexStatement2740 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_createIndexStatement2742 = new BitSet(new long[]{0x000C000000000000L});
    public static final BitSet FOLLOW_IDENT_in_createIndexStatement2760 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_ON_in_createIndexStatement2764 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_createIndexStatement2768 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_createIndexStatement2770 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_createIndexStatement2774 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_createIndexStatement2776 = new BitSet(new long[]{0x0000000010000002L});
    public static final BitSet FOLLOW_K_USING_in_createIndexStatement2788 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_createIndexStatement2792 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createTriggerStatement2826 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_K_TRIGGER_in_createTriggerStatement2828 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_IDENT_in_createTriggerStatement2833 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_ON_in_createTriggerStatement2836 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_createTriggerStatement2840 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_K_USING_in_createTriggerStatement2842 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_createTriggerStatement2846 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropTriggerStatement2877 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_K_TRIGGER_in_dropTriggerStatement2879 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_IDENT_in_dropTriggerStatement2884 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_ON_in_dropTriggerStatement2887 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_dropTriggerStatement2891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterKeyspaceStatement2931 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement2933 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_keyspaceName_in_alterKeyspaceStatement2937 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_WITH_in_alterKeyspaceStatement2947 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_properties_in_alterKeyspaceStatement2949 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2985 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2987 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_alterTableStatement2991 = new BitSet(new long[]{0x03C0010000000000L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement3005 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3009 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement3011 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement3015 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ADD_in_alterTableStatement3031 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3037 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement3041 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_alterTableStatement3064 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3069 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WITH_in_alterTableStatement3109 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_properties_in_alterTableStatement3112 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_RENAME_in_alterTableStatement3145 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3199 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTableStatement3201 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3205 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_alterTableStatement3226 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3230 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTableStatement3232 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTableStatement3236 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTypeStatement3282 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTypeStatement3284 = new BitSet(new long[]{0x4025F80009008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_non_type_ident_in_alterTypeStatement3288 = new BitSet(new long[]{0x0380000000000000L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTypeStatement3302 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3306 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTypeStatement3308 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_alterTypeStatement3312 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ADD_in_alterTypeStatement3328 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3334 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_alterTypeStatement3338 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_RENAME_in_alterTypeStatement3361 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTypeStatement3363 = new BitSet(new long[]{0x4025F80009008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_non_type_ident_in_alterTypeStatement3367 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_RENAME_in_alterTypeStatement3386 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3424 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTypeStatement3426 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3430 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_alterTypeStatement3453 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3457 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTypeStatement3459 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_alterTypeStatement3463 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement3530 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement3532 = new BitSet(new long[]{0x4025F8082B0381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_K_IF_in_dropKeyspaceStatement3535 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_dropKeyspaceStatement3537 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_keyspaceName_in_dropKeyspaceStatement3546 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropTableStatement3580 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropTableStatement3582 = new BitSet(new long[]{0x4025F8082B0381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_K_IF_in_dropTableStatement3585 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_dropTableStatement3587 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_dropTableStatement3596 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropTypeStatement3630 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_dropTypeStatement3632 = new BitSet(new long[]{0x4025F8000B008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_K_IF_in_dropTypeStatement3635 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_dropTypeStatement3637 = new BitSet(new long[]{0x4025F80009008140L,0x0000004000801FF8L});
    public static final BitSet FOLLOW_non_type_ident_in_dropTypeStatement3646 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement3680 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement3682 = new BitSet(new long[]{0x0004000002000000L});
    public static final BitSet FOLLOW_K_IF_in_dropIndexStatement3685 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_EXISTS_in_dropIndexStatement3687 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_IDENT_in_dropIndexStatement3696 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement3727 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_truncateStatement3731 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_GRANT_in_grantStatement3756 = new BitSet(new long[]{0x00C0004000000020L,0x000000000000000EL});
    public static final BitSet FOLLOW_permissionOrAll_in_grantStatement3768 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_ON_in_grantStatement3776 = new BitSet(new long[]{0x4025FA88290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_resource_in_grantStatement3788 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_K_TO_in_grantStatement3796 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_grantStatement3808 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_REVOKE_in_revokeStatement3839 = new BitSet(new long[]{0x00C0004000000020L,0x000000000000000EL});
    public static final BitSet FOLLOW_permissionOrAll_in_revokeStatement3851 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_ON_in_revokeStatement3859 = new BitSet(new long[]{0x4025FA88290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_resource_in_revokeStatement3871 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_FROM_in_revokeStatement3879 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_revokeStatement3891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_listPermissionsStatement3929 = new BitSet(new long[]{0x00C0004000000020L,0x000000000000000EL});
    public static final BitSet FOLLOW_permissionOrAll_in_listPermissionsStatement3941 = new BitSet(new long[]{0x8008000000000002L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_ON_in_listPermissionsStatement3951 = new BitSet(new long[]{0x4025FA88290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_resource_in_listPermissionsStatement3953 = new BitSet(new long[]{0x8000000000000002L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_OF_in_listPermissionsStatement3968 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_listPermissionsStatement3970 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_NORECURSIVE_in_listPermissionsStatement3985 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_permission4021 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALL_in_permissionOrAll4070 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_K_PERMISSIONS_in_permissionOrAll4074 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_permission_in_permissionOrAll4095 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000020L});
    public static final BitSet FOLLOW_K_PERMISSION_in_permissionOrAll4099 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dataResource_in_resource4127 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALL_in_dataResource4150 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_K_KEYSPACES_in_dataResource4152 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dataResource4162 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_keyspaceName_in_dataResource4168 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dataResource4180 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_columnFamilyName_in_dataResource4189 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createUserStatement4229 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_K_USER_in_createUserStatement4231 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_createUserStatement4233 = new BitSet(new long[]{0x0000010000000002L,0x0000000000000300L});
    public static final BitSet FOLLOW_K_WITH_in_createUserStatement4243 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_userOptions_in_createUserStatement4245 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000300L});
    public static final BitSet FOLLOW_K_SUPERUSER_in_createUserStatement4259 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_NOSUPERUSER_in_createUserStatement4265 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterUserStatement4310 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_K_USER_in_alterUserStatement4312 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_alterUserStatement4314 = new BitSet(new long[]{0x0000010000000002L,0x0000000000000300L});
    public static final BitSet FOLLOW_K_WITH_in_alterUserStatement4324 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_userOptions_in_alterUserStatement4326 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000300L});
    public static final BitSet FOLLOW_K_SUPERUSER_in_alterUserStatement4340 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_NOSUPERUSER_in_alterUserStatement4346 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropUserStatement4382 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_K_USER_in_dropUserStatement4384 = new BitSet(new long[]{0x0014000000000000L});
    public static final BitSet FOLLOW_username_in_dropUserStatement4386 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_listUsersStatement4411 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_K_USERS_in_listUsersStatement4413 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_userOption_in_userOptions4433 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_PASSWORD_in_userOption4454 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_userOption4458 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cident4487 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cident4512 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cident4531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_keyspaceName4564 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName4598 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_141_in_columnFamilyName4601 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName4605 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cfOrKsName4626 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cfOrKsName4651 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cfOrKsName4670 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_constant4695 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_constant4707 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FLOAT_in_constant4726 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BOOLEAN_in_constant4747 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UUID_in_constant4766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HEXNUMBER_in_constant4788 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_144_in_constant4806 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_set_in_constant4815 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_145_in_map_literal4844 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x00000000000F4200L});
    public static final BitSet FOLLOW_term_in_map_literal4862 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_146_in_map_literal4864 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_map_literal4868 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080800L});
    public static final BitSet FOLLOW_139_in_map_literal4874 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_map_literal4878 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_146_in_map_literal4880 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_map_literal4884 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080800L});
    public static final BitSet FOLLOW_147_in_map_literal4900 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_146_in_set_or_map4924 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_set_or_map4928 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_set_or_map4944 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_set_or_map4948 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_146_in_set_or_map4950 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_set_or_map4954 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_set_or_map4989 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_set_or_map4993 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_142_in_collection_literal5027 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x000000000007C200L});
    public static final BitSet FOLLOW_term_in_collection_literal5045 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008800L});
    public static final BitSet FOLLOW_139_in_collection_literal5051 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_collection_literal5055 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008800L});
    public static final BitSet FOLLOW_143_in_collection_literal5071 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_145_in_collection_literal5081 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_collection_literal5085 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x00000000000C0800L});
    public static final BitSet FOLLOW_set_or_map_in_collection_literal5089 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_147_in_collection_literal5094 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_145_in_collection_literal5112 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_147_in_collection_literal5114 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_145_in_usertype_literal5158 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_usertype_literal5162 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_146_in_usertype_literal5164 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_usertype_literal5168 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080800L});
    public static final BitSet FOLLOW_139_in_usertype_literal5174 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_usertype_literal5178 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_146_in_usertype_literal5180 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_usertype_literal5184 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080800L});
    public static final BitSet FOLLOW_147_in_usertype_literal5191 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_value5214 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collection_literal_in_value5236 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_usertype_literal_in_value5248 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_NULL_in_value5260 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_146_in_value5284 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_value5288 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QMARK_in_value5305 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_intValue5351 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_146_in_intValue5365 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_intValue5369 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QMARK_in_intValue5379 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_functionName5412 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_function_keyword_in_functionName5446 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TOKEN_in_functionName5456 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_functionArgs5501 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_functionArgs5503 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_functionArgs5513 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_functionArgs5517 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_functionArgs5533 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_functionArgs5537 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_functionArgs5551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_value_in_term5576 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionName_in_term5613 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_functionArgs_in_term5617 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_term5627 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_term5631 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_term5633 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_term5637 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation5660 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_148_in_columnOperation5662 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_columnOperation5666 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_149_in_columnOperation5669 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_columnOperation5673 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation5694 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_148_in_columnOperation5696 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_columnOperation5700 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000210000L});
    public static final BitSet FOLLOW_set_in_columnOperation5704 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_columnOperation5714 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation5732 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_148_in_columnOperation5734 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_columnOperation5738 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_INTEGER_in_columnOperation5742 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation5760 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
    public static final BitSet FOLLOW_142_in_columnOperation5762 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_columnOperation5766 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
    public static final BitSet FOLLOW_143_in_columnOperation5768 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_148_in_columnOperation5770 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_columnOperation5774 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_property_in_properties5800 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_properties5804 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_property_in_properties5806 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_cident_in_property5829 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_148_in_property5831 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFF87FFF8L,0x0000000000030000L});
    public static final BitSet FOLLOW_propertyValue_in_property5836 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_map_literal_in_property5865 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_propertyValue5893 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_propertyValue5915 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_148_in_relationType5938 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_150_in_relationType5949 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_151_in_relationType5960 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_152_in_relationType5970 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_153_in_relationType5981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation6003 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000003D00000L});
    public static final BitSet FOLLOW_relationType_in_relation6007 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_relation6011 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TOKEN_in_relation6021 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_relation6044 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_relation6048 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_relation6054 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_relation6058 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_relation6064 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000003D00000L});
    public static final BitSet FOLLOW_relationType_in_relation6076 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_relation6080 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation6100 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
    public static final BitSet FOLLOW_K_IN_in_relation6102 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L,0x0000000000040000L});
    public static final BitSet FOLLOW_QMARK_in_relation6107 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_146_in_relation6113 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_cident_in_relation6117 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation6140 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
    public static final BitSet FOLLOW_K_IN_in_relation6142 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_relation6153 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074600L});
    public static final BitSet FOLLOW_term_in_relation6159 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_139_in_relation6164 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_relation6168 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000C00L});
    public static final BitSet FOLLOW_138_in_relation6178 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation6190 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
    public static final BitSet FOLLOW_K_CONTAINS_in_relation6192 = new BitSet(new long[]{0x4035F808290781C0L,0x0000007FFFBFFFF8L,0x0000000000074200L});
    public static final BitSet FOLLOW_term_in_relation6208 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_relation6218 = new BitSet(new long[]{0x4025F808290381C0L,0x0000007FFFA01FF8L,0x0000000000000200L});
    public static final BitSet FOLLOW_relation_in_relation6220 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_138_in_relation6223 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_comparatorType6246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collection_type_in_comparatorType6262 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_non_type_ident_in_comparatorType6274 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_comparatorType6287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ASCII_in_native_type6316 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BIGINT_in_native_type6330 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BLOB_in_native_type6343 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BOOLEAN_in_native_type6358 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COUNTER_in_native_type6370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DECIMAL_in_native_type6382 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DOUBLE_in_native_type6394 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_FLOAT_in_native_type6407 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INET_in_native_type6421 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INT_in_native_type6436 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TEXT_in_native_type6452 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_native_type6467 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UUID_in_native_type6477 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_VARCHAR_in_native_type6492 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_VARINT_in_native_type6504 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMEUUID_in_native_type6517 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_MAP_in_collection_type6541 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
    public static final BitSet FOLLOW_150_in_collection_type6544 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_collection_type6548 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_collection_type6550 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_collection_type6554 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000001000000L});
    public static final BitSet FOLLOW_152_in_collection_type6556 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_collection_type6574 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
    public static final BitSet FOLLOW_150_in_collection_type6576 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_collection_type6580 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000001000000L});
    public static final BitSet FOLLOW_152_in_collection_type6582 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SET_in_collection_type6600 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
    public static final BitSet FOLLOW_150_in_collection_type6603 = new BitSet(new long[]{0x4035F808A9008140L,0x0000007FFF801FF8L});
    public static final BitSet FOLLOW_comparatorType_in_collection_type6607 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000001000000L});
    public static final BitSet FOLLOW_152_in_collection_type6609 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_username0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_non_type_ident6669 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_non_type_ident6700 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_basic_unreserved_keyword_in_non_type_ident6725 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_function_keyword_in_unreserved_keyword6750 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unreserved_keyword6766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_basic_unreserved_keyword_in_unreserved_function_keyword6801 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_unreserved_function_keyword6813 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_basic_unreserved_keyword6851 = new BitSet(new long[]{0x0000000000000002L});

}