package com.example.finkcdc.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author: CS
 * @date: 2022/7/19 下午5:49
 * @description:
 */
@FunctionHint(output = @DataTypeHint("Row<S String>") )
public class DatabaseInvoiceVatUDTF extends TableFunction<Row> {




}
