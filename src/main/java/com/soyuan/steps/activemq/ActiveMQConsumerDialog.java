package com.soyuan.steps.activemq;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStreamingDialog;
import org.pentaho.di.ui.util.DialogUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 上午10:32
 */
@SuppressWarnings( {"FieldCanBeLocal","unused" } )
public class ActiveMQConsumerDialog extends BaseStreamingDialog implements StepDialogInterface {
    private static Class<?> PKG = ActiveMQConsumerDialog.class;
    private ActiveMQConsumerMeta consumerMeta;
    private Text wBrokerUrl;
    private Text wQueue;
    /**
     * option tab页
     */
    private CTabItem wOptionsTab;
    private Composite wOptionsComp;
    private TableView optionsTable;
    /**
     * field tab页
     */
    private CTabItem wFieldsTab;
    private Composite wFieldsComp;
    private TableView fieldsTable;

    public ActiveMQConsumerDialog(Shell parent, Object in, TransMeta tr, String sname) {
        super(parent, in, tr, sname);
        this.consumerMeta = (ActiveMQConsumerMeta) in;
    }

    @Override
    protected String getDialogTitle() {
        return BaseMessages.getString(PKG, "ActiveMQConsumerDialog.Shell.Title");
    }

    @Override
    protected void buildSetup(Composite wSetupComp) {
        props.setLook( wSetupComp );

        FormLayout setupLayout = new FormLayout();
        setupLayout.marginHeight = 15;
        setupLayout.marginWidth = 15;
        wSetupComp.setLayout( setupLayout );

        final Label wlBrokerUrl = new Label(wSetupComp, SWT.LEFT);
        props.setLook(wlBrokerUrl);
        wlBrokerUrl.setText(BaseMessages.getString(PKG,"ActiveMQConsumerInputDialog.BrokerUrl"));
        FormData fdlBrokerUrl = new FormData();
        fdlBrokerUrl.left = new FormAttachment(0, 0);
        fdlBrokerUrl.top = new FormAttachment(0,0);
        wlBrokerUrl.setLayoutData(fdlBrokerUrl);

        wBrokerUrl = new Text(wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wBrokerUrl);
        FormData fdBrokerUrl = new FormData();
        fdBrokerUrl.left = new FormAttachment(wlBrokerUrl,5);
        fdBrokerUrl.top = new FormAttachment(0, 0);
        fdBrokerUrl.width = 200;
        wBrokerUrl.setLayoutData(fdBrokerUrl);

        //队列标签
        final Label wlQueue = new Label(wSetupComp, SWT.LEFT);
        props.setLook(wlQueue);
        wlQueue.setText(BaseMessages.getString(PKG,"ActiveMQConsumerInputDialog.Queue"));
        FormData fdlQueue = new FormData();
        fdlQueue.left = new FormAttachment(0, 0);
        fdlQueue.top = new FormAttachment(wlBrokerUrl,30);
        wlQueue.setLayoutData(fdlQueue);

        wQueue = new Text(wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wQueue);
        FormData fdQueue = new FormData();
        fdQueue.left = new FormAttachment(wlQueue,5);
        fdQueue.top = new FormAttachment(wlBrokerUrl, 30);
        fdQueue.width = 200;
        wQueue.setLayoutData(fdQueue);

        FormData fdSetupComp = new FormData();
        fdSetupComp.left = new FormAttachment( 0, 0 );
        fdSetupComp.top = new FormAttachment( 0, 0 );
        fdSetupComp.right = new FormAttachment( 100, 0 );
        fdSetupComp.bottom = new FormAttachment( 100, 0 );
        wSetupComp.setLayoutData( fdSetupComp );

        wSetupComp.layout();
        wSetupTab.setControl(wSetupComp);
    }

    /**
     * 打开界面时,将meta的值设置到输入框中
     */
    @Override
    protected void getData() {
        super.getData();
        if (consumerMeta.getBrokerUrl() != null) {
            wBrokerUrl.setText(consumerMeta.getBrokerUrl());
        }
        if (consumerMeta.getQueue() != null) {
            wQueue.setText(consumerMeta.getQueue());
        }
        switch ( specificationMethod ) {
            case FILENAME:
                wTransPath.setText(Const.NVL(meta.getFileName(), ""));
                break;
            case REPOSITORY_BY_NAME:
                String fullPath = Const.NVL(meta.getDirectoryPath(), "") + "/" + Const.NVL(meta.getTransName(), "");
                wTransPath.setText(fullPath);
                break;
            case REPOSITORY_BY_REFERENCE:
                referenceObjectId = meta.getTransObjectId();
                getByReferenceData(referenceObjectId);
                break;
            default:
                break;
        }
        populateFieldData();
    }

    @Override
    protected void createAdditionalTabs() {
        buildFieldsTab();
        createOptionsTab();
    }

    /****************start field tab**********************/
    private void buildFieldsTab() {
        wFieldsTab = new CTabItem(wTabFolder, SWT.NONE, 2);
        wFieldsTab.setText(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.FieldsTab"));

        wFieldsComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wFieldsComp);
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wFieldsComp.setLayout(fieldsLayout);

        FormData fieldsFormData = new FormData();
        fieldsFormData.left = new FormAttachment(0, 0);
        fieldsFormData.top = new FormAttachment(wFieldsComp, 0);
        fieldsFormData.right = new FormAttachment(100, 0);
        fieldsFormData.bottom = new FormAttachment(100, 0);
        wFieldsComp.setLayoutData(fieldsFormData);

        buildFieldTable(wFieldsComp, wFieldsComp);

        wFieldsComp.layout();
        wFieldsTab.setControl(wFieldsComp);
    }

    private void buildFieldTable(Composite parentWidget, Composite relativePosition) {
        ColumnInfo[] columns = getFieldColumns();
        //active: text messageId timestamp 共3个属性
        int rows = ActiveMQConsumerField.Name.values().length;

        fieldsTable = new TableView(transMeta, parentWidget, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                columns, rows, true, lsMod, props, false);
        fieldsTable.setSortable(false);
        fieldsTable.getTable().addListener(SWT.Resize, event -> {
            Table table = (Table) event.widget;
            table.getColumn(1).setWidth(147);
            table.getColumn(2).setWidth(147);
            table.getColumn(3).setWidth(147);
        });

        populateFieldData();

        FormData fdData = new FormData();
        fdData.left = new FormAttachment(0, 0);
        fdData.top = new FormAttachment(relativePosition, 5);
        fdData.right = new FormAttachment(100, 0);

        // resize the columns to fit the data in them
        stream(fieldsTable.getTable().getColumns()).forEach(column -> {
            if (column.getWidth() > 0) {
                // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
                column.setWidth(120);
            }
        });

        // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
        fieldsTable.setReadonly(true);
        fieldsTable.setLayoutData(fdData);
    }

    /**
     * 获取fields列
     *
     * @return
     */
    private ColumnInfo[] getFieldColumns() {
        //参数说明: 1-列名  2-类型 3-是否数字 4-是否只读
        ColumnInfo input = new ColumnInfo(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.Column.Input"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, true);
        ColumnInfo output = new ColumnInfo(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.Column.Output"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false);

        final ActiveMQConsumerField.Type[] values = ActiveMQConsumerField.Type.values();
        String[] combos = stream(values).map(ActiveMQConsumerField.Type::toString).toArray(String[]::new);
        //类型COLUMN_TYPE_CCOMBO-表示下拉框
        ColumnInfo type = new ColumnInfo(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.Column.Type"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, combos, false);
        //禁止编辑type列
        type.setDisabledListener(rowNr -> true);


        return new ColumnInfo[]{input, output, type};
    }

    private void populateFieldData() {
        final List<ActiveMQConsumerField> fieldDefinitions = consumerMeta.getFieldDefinitions();
        int rowIndex = 0;
        for (ActiveMQConsumerField field : fieldDefinitions) {
            TableItem key = fieldsTable.getTable().getItem(rowIndex++);

            if (field.getInputName() != null) {
                key.setText(1, field.getInputName().toString());
            }

            if (field.getOutputName() != null) {
                key.setText(2, field.getOutputName());
            }

            if (field.getOutputType() != null) {
                key.setText(3, field.getOutputType().toString());
            }
        }
    }

    /****************end field tab**********************/

    /****************start option tab**********************/
    private void createOptionsTab() {
        wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
        wOptionsTab.setText(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.OptionsTab"));

        wOptionsComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wOptionsComp);

        //设置外边框布局
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wOptionsComp.setLayout(fieldsLayout);
        //设置内容布局
        FormData optionsFormData = new FormData();
        optionsFormData.left = new FormAttachment(0, 0);
        optionsFormData.top = new FormAttachment(wOptionsComp, 0);
        optionsFormData.right = new FormAttachment(100, 0);
        optionsFormData.bottom = new FormAttachment(100, 0);
        wOptionsComp.setLayoutData(optionsFormData);

        buildOptionsTable(wOptionsComp);

        wOptionsComp.layout();
        wOptionsTab.setControl(wOptionsComp);
    }

    private void buildOptionsTable(Composite wOptionsComp) {
        ColumnInfo[] columns = getOptionsColumns();
        //设置默认值
        if (consumerMeta.getConfig().size() == 0) {
            Map<String, String> advancedConfig = new LinkedHashMap<>();
            advancedConfig.put(ActiveMQConfig.USERNAME, "");
            advancedConfig.put(ActiveMQConfig.PASSWORD, "");
            consumerMeta.setConfig(advancedConfig);
        }
        //有几行数据
        int fieldCount = consumerMeta.getConfig().size();

        optionsTable = new TableView(transMeta, wOptionsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                columns, fieldCount, false, lsMod, props, false);
        //不排序
        optionsTable.setSortable(false);
        //调整大小事件
        optionsTable.getTable().addListener(SWT.Resize, event -> {
            Table table = (Table) event.widget;
            table.getColumn(1).setWidth(220);
            table.getColumn(2).setWidth(220);
        });
        //填充数据到可视化表格中
        populateOptionsData();
        //表格的数据布局
        FormData fdData = new FormData();
        fdData.left = new FormAttachment(0, 0);
        fdData.top = new FormAttachment(0, 0);
        fdData.right = new FormAttachment(100, 0);
        fdData.bottom = new FormAttachment(100, 0);

        // 调整列
        stream(optionsTable.getTable().getColumns()).forEach(column -> {
            if (column.getWidth() > 0) {
                column.setWidth(120);
            }
        });
        //设置表格的数据布局
        optionsTable.setLayoutData(fdData);
    }

    /**
     * 填充数据到可视化表格中
     */
    private void populateOptionsData() {
        int rowIndex = 0;
        for ( Map.Entry<String, String> entry : consumerMeta.getConfig().entrySet() ) {
            TableItem item = optionsTable.getTable().getItem( rowIndex++ );
            item.setText(1,entry.getKey());
            item.setText(2,entry.getValue());
        }
    }

    /**
     * 获取options tab的列-Name,Value
     * @return
     */
    private ColumnInfo[] getOptionsColumns() {
        ColumnInfo optionName = new ColumnInfo( BaseMessages.getString( PKG, "ActiveMQConsumerInputDialog.NameField" ),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false );

        ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "ActiveMQConsumerInputDialog.Column.Value" ),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false );
        //可以使用变量替换
        value.setUsingVariables( true );
        return new ColumnInfo[]{ optionName, value };
    }
    /****************end option tab**********************/

    /**
     * 点击确认时,将界面的值设置到meta中
     * @param meta
     */
    @Override
    protected void additionalOks(BaseStreamStepMeta meta) {
        consumerMeta.setBrokerUrl(wBrokerUrl.getText());
        consumerMeta.setQueue(wQueue.getText());
        //将field值设置到meta中
        setFieldsFromTable();
        //将option中的值设置到meta中
        setOptionsFromTable();
    }

    /**
     * 将field值设置到meta中
     */
    private void setFieldsFromTable() {
        int itemCount = fieldsTable.getItemCount();
        for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
            TableItem row = fieldsTable.getTable().getItem(rowIndex);
            String inputName = row.getText(1);
            String outputName = row.getText(2);
            String outputType = row.getText(3);

            final ActiveMQConsumerField.Name ref = ActiveMQConsumerField.Name.valueOf(inputName.toUpperCase());

            final ActiveMQConsumerField field = new ActiveMQConsumerField(ref, outputName,
                    ActiveMQConsumerField.Type.valueOf(outputType));
            consumerMeta.setField(field);
        }
    }

    /**
     * 将option中的值设置到meta中
     */
    private void setOptionsFromTable() {
        int itemCount = optionsTable.getItemCount();
        Map<String, String> advancedConfig = new LinkedHashMap<>();
        for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
            TableItem row = optionsTable.getTable().getItem(rowIndex);
            String config = row.getText(1);
            String value = row.getText(2);
            //不为空才设置到meta中
            if (!StringUtils.isBlank(config) && !advancedConfig.containsKey(config)) {
                advancedConfig.put(config, value);
            }
        }
        consumerMeta.setConfig(advancedConfig);
    }

    @Override
    protected int[] getFieldTypes() {
        TableItem[] rows = fieldsTable.getTable().getItems();
        int[] types = new int[rows.length];
        for (int i = 0; i < rows.length; i++) {
            TableItem item = rows[i];
            String type = item.getText(3);
            int idType = ValueMetaFactory.getIdForValueMeta(type);
            types[i] = idType;
        }
       return types;
    }

    @Override
    protected String[] getFieldNames() {
        TableItem[] rows = fieldsTable.getTable().getItems();
        String[] names = new String[rows.length];
        for (int i = 0; i < rows.length; i++) {
            TableItem item = rows[i];
            String name = item.getText(2);
            names[i] = name;
        }
        return names;
    }

    private void getByReferenceData( ObjectId transObjectId ) {
        try {
            RepositoryObject transInf = repository.getObjectInformation( transObjectId, RepositoryObjectType.TRANSFORMATION );
            String
                    path =
                    DialogUtils
                            .getPath( transMeta.getRepositoryDirectory().getPath(),
                                    transInf.getRepositoryDirectory().getPath() );
            String fullPath = Const.NVL( path, "" ) + "/" + Const.NVL( transInf.getName(), "" );
            wTransPath.setText( fullPath );
        } catch ( KettleException e ) {
            new ErrorDialog( shell,
                    BaseMessages.getString( PKG, "JobEntryTransDialog.Exception.UnableToReferenceObjectId.Title" ),
                    BaseMessages.getString( PKG, "JobEntryTransDialog.Exception.UnableToReferenceObjectId.Message" ), e );
        }
    }

}
