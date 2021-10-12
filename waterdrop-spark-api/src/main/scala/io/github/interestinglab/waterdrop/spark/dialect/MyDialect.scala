package io.github.interestinglab.waterdrop.spark.dialect

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

import java.sql.Types
import java.util.TimeZone

/**
 * @description: 自定义 MySQL Dialect 类, 扩展 Spark 对 MySQL 不支持的数据类型
 * @author: Kino
 * @create: 2021-03-31 19:55:33
 */
object MyDialect {
  def useMyJdbcDialect(jdbcUrl: String): Unit = {
    // 将当前的 JdbcDialect 对象unregistered掉
    JdbcDialects.unregisterDialect(JdbcDialects.get(jdbcUrl))

    if (jdbcUrl.contains("jdbc:mysql")) {
      JdbcDialects.registerDialect(new customMySqlJdbcDialect)
    } else if (jdbcUrl.contains("jdbc:postgresql")) {

    } else if (jdbcUrl.contains("jdbc:sqlserver")) {

    } else if (jdbcUrl.contains("jdbc:oracle")) {
      JdbcDialects.registerDialect(new customOracleJdbcDialect)
    } else if (jdbcUrl.contains("jdbc:informix")) {

    }
  }
}

class customMySqlJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:mysql")

  /**
   * 从 MySQL 读取数据到 Spark 的数据类型转换映射.
   *
   * @param sqlType
   * @param typeName
   * @param size
   * @param md
   * @return
   */
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else if (sqlType == Types.TIMESTAMP || sqlType == Types.DATE || sqlType == Types.TIME || sqlType == -101 || sqlType == -102) {
      // 将不支持的 Timestamp with local Timezone 以TimestampType形式返回
      // Some(TimestampType)
      Some(StringType)
    } else if (sqlType == Types.BLOB) {
      Some(BinaryType)
    } else None
  }

  /**
   * 从 Spark(DataType) 到 MySQL(SQLType) 的数据类型映射
   *
   * @param dt
   * @return
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      // case StringType => Some(JdbcType("VARCHAR2(2000)", java.sql.Types.VARCHAR))
      // case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
      // case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.NUMERIC))
      // case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.NUMERIC))
      // case DoubleType => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
      // case FloatType => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
      // case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
      // case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
      // case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
      // case TimestampType => Some(JdbcType("DATE", java.sql.Types.TIMESTAMP))
      // case DateType => Some(JdbcType("VARCHAR2(2000)", java.sql.Types.DATE))
      // case _ => None

      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = {
    Some(false)
  }
}

class customOracleJdbcDialect extends JdbcDialect {
  val BINARY_FLOAT = 100
  val BINARY_DOUBLE = 101
  val TIMESTAMPTZ = -101

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case Types.NUMERIC =>
        val scale = if (null != md) md.build().getLong("scale") else 0L
        size match {
          // Handle NUMBER fields that have no precision/scale in special way
          // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
          // For more details, please see
          // https://github.com/apache/spark/pull/8780#issuecomment-145598968
          // and
          // https://github.com/apache/spark/pull/8780#issuecomment-144541760
          case 0 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
          // this to NUMERIC with -127 scale
          // Not sure if there is a more robust way to identify the field as a float (or other
          // numeric types that do not specify a scale.
          case _ if scale == -127L => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          case _ => None
        }
      case TIMESTAMPTZ if supportTimeZoneTypes
      => Some(TimestampType) // Value for Timestamp with Time Zone in Oracle
      case BINARY_FLOAT => Some(FloatType) // Value for OracleTypes.BINARY_FLOAT
      case BINARY_DOUBLE => Some(DoubleType) // Value for OracleTypes.BINARY_DOUBLE
      case _ => None
    }
  }

  private def supportTimeZoneTypes: Boolean = {
    val timeZone = DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone)
    // TODO: support timezone types when users are not using the JVM timezone, which
    // is the default value of SESSION_LOCAL_TIMEZONE
    timeZone == TimeZone.getDefault
  }

}