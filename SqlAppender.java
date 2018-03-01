import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author ido
 * Date: 2018/1/29
 **/
@Slf4j
public class SqlAppender {
    private EntityManager entityManager;
    private StringBuilder sql;
    private final String initSql;
    private final static String ALIAS_SUFFIX = "}";
    private final static String ALIAS_PREFFIX = "${";

    private final Map<String, Object> paramAndValueMap = new HashMap<>();
    //store the sql and the original sql column and own define columns alias
    private final static Map<String, Map<String, Object>> sqlToNativeAndAliasMap = new ConcurrentHashMap<>();


    private final String ALIAS_REPLACE_REGEX = "\\$\\{\\s*\\w+\\s*\\}";

    private static LoadingCache<String, List<String>> sqlColumnsMap = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<String, List<String>>() {
                @Override
                public List<String> load(String sql) throws Exception {
                    return getColumnAlias(sql);
                }
            });

    public SqlAppender(EntityManager entityManager, StringBuilder sql) {
        Objects.requireNonNull(entityManager);
        Objects.requireNonNull(sql);
        this.entityManager = entityManager;
        this.sql = sql;
        this.initSql = sql.toString();
    }

    public SqlAppender and(String columnName, String paramName, Object paramVal) {
        return and(columnName, "=", paramName, paramVal);
    }

    public SqlAppender and(String columnName, String op, String paramName, Object paramVal) {
        return logicOp("and", columnName, op, paramName, paramVal);

    }

    public SqlAppender or(String columnName, String op, String paramName, Object paramVal) {

        return logicOp("or", columnName, op, paramName, paramVal);

    }


    public SqlAppender orderBy(List<? extends Sorter> sorters) {
        if (sorters == null || sorters.size() == 0) {
            return this;
        }
        //first time call order by
        for (Sorter sort : sorters) {
            orderBy(sort.sortField(), sort.isDesc());
        }
        return this;

    }

    public SqlAppender orderBy(String columnName, boolean desc) {
        if (columnName == null || columnName.isEmpty()) {
            return this;
        }
        //first time call order by
        if (!sql.toString().toLowerCase().contains("order by ")) {

            sql.append(" order by ")
                    .append(sqlToNativeAndAliasMap.get(this.initSql).get(columnName));
            if (desc) {
                sql.append(" desc ");
            } else {
                sql.append(" asc ");
            }

        } else {
            sql.append(" , ")
                    .append(sqlToNativeAndAliasMap.get(this.initSql).get(columnName));
            if (desc) {
                sql.append(" desc ");
            } else {
                sql.append(" asc ");
            }

        }
        return this;

    }


    public SqlAppender limit(int offset, int pageSize) {
        sql.append(" limit ")
                .append(offset)
                .append(" , ")
                .append(pageSize);

        return this;

    }


    private SqlAppender logicOp(String logicOp, String columnName, String op, String paramName, Object paramVal) {


        if (paramVal != null) {
            if (paramVal instanceof Collection && ((Collection) paramVal).size() == 0) {
                return this;
            }

            Objects.requireNonNull(columnName);
            Objects.requireNonNull(op);
            Objects.requireNonNull(paramName);


            sql.append(" ")
                    .append(logicOp)
                    .append(" ")
                    .append(columnName)
                    .append(" ")
                    .append(op)
                    .append(" :")
                    .append(paramName);


            paramAndValueMap.put(paramName, paramVal);
        }


        return this;
    }

    /**
     * select out the column alias from sql
     * alias format : ${xxx}
     *
     * @return alias list result
     */
    private static List<String> getColumnAlias(String sql) {
        log.info("fetching column alias...");
        String s = sql.toString();


        int selectIndex = s.indexOf("select");
        if (selectIndex == -1) {
            selectIndex = s.indexOf("SELECT");
        }

        int fromIndex = s.indexOf("from");
        if (fromIndex == -1) {
            fromIndex = s.indexOf("FROM");
        }

        if (selectIndex == -1 || fromIndex == -1) {
            throw new RuntimeException("can not find 'select' or 'from' key word from your sql !");
        }


        String selectColumn = s.substring(selectIndex + "select".length(), fromIndex);
        //after split format columns like this:  a.column as alias ${voAlias}
        String[] columns = selectColumn.split(",");
        List<String> resultColumns = new ArrayList<>(columns.length);
        final Map<String, Object> columnToAlias = new HashMap<>(columns.length);
        for (String cln : columns) {
            ColumnEntry ce = getColumnEntry(cln);
            if(ce.voColumn == null){
                log.error("view column name can not be parsed ，vo name {}, original column name {}  " ,ce.voColumn,ce.originalColumn);
                throw new RuntimeException("view column name can not be parsed "+ ce.originalColumn);
            }
            columnToAlias.put(ce.voColumn, ce.originalColumn);
            resultColumns.add(ce.voColumn);
        }
        sqlToNativeAndAliasMap.put(sql, columnToAlias);

        if (log.isDebugEnabled()) {
            log.debug("column alias for {} is {}", sql, resultColumns.toString());
        }
        return resultColumns;
    }

    public Integer count() {
        final String sqlToExecute = sql.toString();
        log.info("\n" + sqlToExecute);
        Query query = this.entityManager.createNativeQuery(sqlToExecute);
        for (Map.Entry<String, Object> entry : paramAndValueMap.entrySet()) {
            query.setParameter(entry.getKey(), entry.getValue());
        }

        return ((BigInteger) query.getSingleResult()).intValue();

    }

    public List getResultList() {
        final String sqlToExecute = sql.toString().replaceAll(ALIAS_REPLACE_REGEX, " ");
        log.info("\n" + sqlToExecute);
        Query query = this.entityManager.createNativeQuery(sqlToExecute);

        for (Map.Entry<String, Object> entry : paramAndValueMap.entrySet()) {
            query.setParameter(entry.getKey(), entry.getValue());
        }

        List<String> columnAlias = null;
        try {
            columnAlias = sqlColumnsMap.get(initSql);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        List originalDataInDb = query.getResultList();

        List<Map<String, Object>> resultMap = new ArrayList<>(originalDataInDb.size());
        for (Object obs : originalDataInDb) {
            Map<String, Object> mp = new HashMap<>();
            for (int i = 0; i < columnAlias.size(); i++) {
                Object[] ob = (Object[]) obs;
                mp.put(columnAlias.get(i), ob[i]);
            }

            resultMap.add(mp);
        }

        return resultMap;
    }


    public static void main(String[] args) {

        String sql = "select c.id as client_id ${ clientId } , c.name as client_name ${clientName} from client c ;";
        log.info(sql.replaceAll("\\$\\{\\s*\\w+\\s*\\}", ""));

        int selectIndex = sql.indexOf("select");
        int fromIndex = sql.indexOf("from");
        String selectColumn = sql.substring(selectIndex + "select".length(), fromIndex);

        log.info(selectColumn);


    }

    private static class ColumnEntry {
        String originalColumn;
        String voColumn;

    }


    private static ColumnEntry getColumnEntry(String s) {
        s = s.trim();
        s = s.replaceAll("(\\n|\\t) ","");
        if (s.indexOf("as") != -1) {
            //format may look like this: a.column  as xxx
            String[] ss = s.split("as");
            ColumnEntry ce = new ColumnEntry();
            ce.originalColumn = ss[0].trim();
            ce.voColumn = ss[1].trim();
            return ce;
        }

        int start = s.indexOf(ALIAS_PREFFIX);
        if(start == -1){
            String[] ogClAndAlias = s.trim().split(" ");
            if(ogClAndAlias.length > 1){
                //format may look like this: a.column  xxx
                ColumnEntry ce = new ColumnEntry();
                ce.originalColumn = ogClAndAlias[0].trim();
                ce.voColumn = ogClAndAlias[1].trim();
                return ce;
            }else{
                //format may look like this: a.column
                //convert ab_cd to abCd format
                ColumnEntry ce = new ColumnEntry();
                String orgCln = s.substring(s.indexOf(".")+1);
                int indexOf_ = orgCln.indexOf("_");
                if(indexOf_ == -1){
                    ce.originalColumn = orgCln;
                    ce.voColumn = orgCln;
                }else{
                    String[] ss = orgCln.split("_");
                    StringBuilder cln = new StringBuilder();
                    //
                    for(int i = 0 ; i < ss.length ; i++){
                        if(i == 0){
                            cln.append(ss[i]);
                            continue;
                        }
                        cln.append(ss[i].substring(0,1).toUpperCase()+ss[i].substring(1));
                    }
                    ce.voColumn = cln.toString();
                    ce.originalColumn = orgCln;
                }
                return ce;
            }
        }
        String pres = s.substring(0, start);
        String[] ogClAndAlias = pres.trim().split(" ");


        if (ogClAndAlias.length > 1) {
            //column and alias
            ColumnEntry ce = new ColumnEntry();
            ce.originalColumn = ogClAndAlias[0].trim();
            int end = s.indexOf(ALIAS_SUFFIX);
            ce.voColumn = s.substring(start + 2, end).trim();
            return ce;

        } else {
            //format may look like this: a.column ${ xxx }
            ColumnEntry ce = new ColumnEntry();
            int end = s.indexOf(ALIAS_SUFFIX);
            ce.voColumn = s.substring(start + 2, end).trim();
            ce.originalColumn = s.substring(0, start).trim();
            return ce;
        }


    }


}
