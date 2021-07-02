package com.soyuan.steps.activemq;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/30 下午5:48
 */
public class ActiveMQProductDialog extends BaseStepDialog implements StepDialogInterface {

    private static final int SHELL_MIN_WIDTH = 527;
    private static final int SHELL_MIN_HEIGHT = 569;
    private static final int INPUT_WIDTH = 350;
    private static Class<?> PKG = ActiveMQProductDialog.class;
    private ActiveMQProductMeta meta;
    private ModifyListener lsMod;
    private CTabFolder wTabFolder;
    private CTabItem wSetupTab;
    private Composite wSetupComp;
    private Text wBrokerUrl;
    private Text wQueue;
    private Label wlMsgField;
    private ComboVar wMsgField;

    private CTabItem wOptionsTab;
    private Composite wOptionsComp;
    private TableView optionsTable;


    public ActiveMQProductDialog(Shell parent, Object in, TransMeta transMeta, String stepname) {
        super(parent, (BaseStepMeta) in, transMeta, stepname);
        this.meta = (ActiveMQProductMeta) in;
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
        changed = meta.hasChanged();

        lsMod = e -> meta.setChanged();
        lsCancel = e -> cancel();
        lsOK = e -> ok();
        lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
        props.setLook(shell);
        setShellImage(shell, meta);
        shell.setMinimumSize(SHELL_MIN_WIDTH, SHELL_MIN_HEIGHT);
        shell.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.Shell.Title"));

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 15;
        formLayout.marginHeight = 15;
        shell.setLayout(formLayout);
        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        Label wicon = new Label(shell, SWT.RIGHT);
        wicon.setImage(getImage());
        FormData fdlicon = new FormData();
        fdlicon.top = new FormAttachment(0, 0);
        fdlicon.right = new FormAttachment(100, 0);
        wicon.setLayoutData(fdlicon);
        props.setLook(wicon);

        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.top = new FormAttachment(0, 0);
        wlStepname.setLayoutData(fdlStepname);

        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.width = 250;
        fdStepname.left = new FormAttachment(0, 0);
        fdStepname.top = new FormAttachment(wlStepname, 5);
        wStepname.setLayoutData(fdStepname);
        wStepname.addSelectionListener(lsDef);

        Label topSeparator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
        FormData fdSpacer = new FormData();
        fdSpacer.height = 2;
        fdSpacer.left = new FormAttachment(0, 0);
        fdSpacer.top = new FormAttachment(wStepname, 15);
        fdSpacer.right = new FormAttachment(100, 0);
        topSeparator.setLayoutData(fdSpacer);

        // Start of tabbed display
        wTabFolder = new CTabFolder(shell, SWT.BORDER);
        props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
        wTabFolder.setSimple(false);
        wTabFolder.setUnselectedCloseVisible(true);

        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        FormData fdCancel = new FormData();
        fdCancel.right = new FormAttachment(100, 0);
        fdCancel.bottom = new FormAttachment(100, 0);
        wCancel.setLayoutData(fdCancel);
        wCancel.addListener(SWT.Selection, lsCancel);

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        FormData fdOk = new FormData();
        fdOk.right = new FormAttachment(wCancel, -5);
        fdOk.bottom = new FormAttachment(100, 0);
        wOK.setLayoutData(fdOk);
        wOK.addListener(SWT.Selection, lsOK);

        Label bottomSeparator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
        props.setLook(bottomSeparator);
        FormData fdBottomSeparator = new FormData();
        fdBottomSeparator.height = 2;
        fdBottomSeparator.left = new FormAttachment(0, 0);
        fdBottomSeparator.bottom = new FormAttachment(wCancel, -15);
        fdBottomSeparator.right = new FormAttachment(100, 0);
        bottomSeparator.setLayoutData(fdBottomSeparator);

        FormData fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment(0, 0);
        fdTabFolder.top = new FormAttachment(topSeparator, 15);
        fdTabFolder.bottom = new FormAttachment(bottomSeparator, -15);
        fdTabFolder.right = new FormAttachment(100, 0);
        wTabFolder.setLayoutData(fdTabFolder);

        buildSetupTab();
        buildOptionsTab();

        getData();
        setSize();

        meta.setChanged(changed);

        wTabFolder.setSelection(0);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return stepname;
    }

    private void buildSetupTab() {
        wSetupTab = new CTabItem(wTabFolder, SWT.NONE);
        wSetupTab.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.SetupTab"));

        wSetupComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wSetupComp);
        FormLayout setupLayout = new FormLayout();
        setupLayout.marginHeight = 15;
        setupLayout.marginWidth = 15;
        wSetupComp.setLayout(setupLayout);

        /*** start 自定义 ***/
        final Label wlBrokerUrl = new Label(wSetupComp, SWT.LEFT);
        props.setLook(wlBrokerUrl);
        wlBrokerUrl.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.BrokerUrl"));
        FormData fdlBrokerUrl = new FormData();
        fdlBrokerUrl.left = new FormAttachment(0, 0);
        fdlBrokerUrl.top = new FormAttachment(0, 0);
        wlBrokerUrl.setLayoutData(fdlBrokerUrl);

        wBrokerUrl = new Text(wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wBrokerUrl);
        FormData fdBrokerUrl = new FormData();
        fdBrokerUrl.left = new FormAttachment(wlBrokerUrl, 5);
        fdBrokerUrl.top = new FormAttachment(0, 0);
        fdBrokerUrl.width = 200;
        wBrokerUrl.setLayoutData(fdBrokerUrl);

        //队列标签
        final Label wlQueue = new Label(wSetupComp, SWT.LEFT);
        props.setLook(wlQueue);
        wlQueue.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.Queue"));
        FormData fdlQueue = new FormData();
        fdlQueue.left = new FormAttachment(0, 0);
        fdlQueue.top = new FormAttachment(wlBrokerUrl, 30);
        wlQueue.setLayoutData(fdlQueue);

        wQueue = new Text(wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wQueue);
        FormData fdQueue = new FormData();
        fdQueue.left = new FormAttachment(wlQueue, 5);
        fdQueue.top = new FormAttachment(wlBrokerUrl, 30);
        fdQueue.width = 200;
        wQueue.setLayoutData(fdQueue);

        wlMsgField = new Label(wSetupComp, SWT.LEFT);
        props.setLook(wlMsgField);
        wlMsgField.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.MsgField"));
        FormData fdlMsgField = new FormData();
        fdlMsgField.left = new FormAttachment(0, 0);
        fdlMsgField.top = new FormAttachment(wQueue, 30);
        fdlMsgField.right = new FormAttachment(50, 0);
        wlMsgField.setLayoutData(fdlMsgField);

        wMsgField = new ComboVar(transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMsgField);
        wMsgField.addModifyListener(lsMod);
        FormData fdMsgField = new FormData();
        fdMsgField.left = new FormAttachment(0, 0);
        fdMsgField.top = new FormAttachment(wlMsgField, 5);
        fdMsgField.right = new FormAttachment(0, INPUT_WIDTH);
        wMsgField.setLayoutData(fdMsgField);
        Listener lsMsgFocus = event -> {
            String current = wMsgField.getText();
            wMsgField.getCComboWidget().removeAll();
            wMsgField.setText(current);
            //从前个步骤获取字段
            try {
                RowMetaInterface rmi = transMeta.getPrevStepFields(stepname);
                final List<ValueMetaInterface> ls = rmi.getValueMetaList();
                for (int i = 0; i < ls.size(); i++) {
                    final ValueMetaBase vmb = (ValueMetaBase) ls.get(i);
                    wMsgField.add(vmb.getName());
                }
            } catch (KettleStepException e) {
                e.printStackTrace();
            }
        };
        wMsgField.getCComboWidget().addListener(SWT.FocusIn, lsMsgFocus);


        /*** end 自定义 ***/

        FormData fdSetupComp = new FormData();
        fdSetupComp.left = new FormAttachment(0, 0);
        fdSetupComp.top = new FormAttachment(0, 0);
        fdSetupComp.right = new FormAttachment(100, 0);
        fdSetupComp.bottom = new FormAttachment(100, 0);
        wSetupComp.setLayoutData(fdSetupComp);
        wSetupComp.layout();
        wSetupTab.setControl(wSetupComp);
    }

    private void buildOptionsTab() {
        wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
        wOptionsTab.setText(BaseMessages.getString(PKG, "ActiveMQProductDialog.Options.Tab"));

        wOptionsComp = new Composite(wTabFolder, SWT.NONE);
        props.setLook(wOptionsComp);
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wOptionsComp.setLayout(fieldsLayout);

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
        if (meta.getConfig().size() == 0) {
            Map<String, String> advancedConfig = new LinkedHashMap<>();
            advancedConfig.put(ActiveMQConfig.USERNAME, "");
            advancedConfig.put(ActiveMQConfig.PASSWORD, "");
            meta.setConfig(advancedConfig);
        }

        //有几行数据
        int fieldCount = meta.getConfig().size();

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

    private ColumnInfo[] getOptionsColumns() {
        ColumnInfo optionName = new ColumnInfo(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.NameField"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false);

        ColumnInfo value = new ColumnInfo(BaseMessages.getString(PKG, "ActiveMQConsumerInputDialog.Column.Value"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false);
        //可以使用变量替换
        value.setUsingVariables(true);
        return new ColumnInfo[]{optionName, value};
    }

    private void populateOptionsData() {
        int rowIndex = 0;
        for (Map.Entry<String, String> entry : meta.getConfig().entrySet()) {
            TableItem item = optionsTable.getTable().getItem(rowIndex++);
            item.setText(1, entry.getKey());
            item.setText(2, entry.getValue());
        }
    }

    /**
     * 将meta中的数据设置到widget中
     */
    private void getData() {
        if (meta.getBrokerUrl() != null) {
            wBrokerUrl.setText(meta.getBrokerUrl());
        }
        if (meta.getQueue() != null) {
            wQueue.setText(meta.getQueue());
        }
        if (meta.getMsgField() != null) {
            wMsgField.setText(meta.getMsgField());
        }
    }

    private Image getImage() {
        PluginInterface plugin =
                PluginRegistry.getInstance().getPlugin(StepPluginType.class, stepMeta.getStepMetaInterface());
        String id = plugin.getIds()[0];
        if (id != null) {
            return GUIResource.getInstance().getImagesSteps().get(id).getAsBitmapForSize(shell.getDisplay(),
                    ConstUI.ICON_SIZE, ConstUI.ICON_SIZE);
        }
        return null;
    }

    private void ok() {
        stepname = wStepname.getText();
        meta.setBrokerUrl(wBrokerUrl.getText());
        meta.setQueue(wQueue.getText());
        meta.setMsgField(wMsgField.getText());
        setOptionsFromTable();
        dispose();
    }

    private void cancel() {
        meta.setChanged(false);
        dispose();
    }

    /**
     * 读取option tab中的值设置到meta config中
     */
    private void setOptionsFromTable() {
        Map<String, String> advancedConfig = new LinkedHashMap<>();
        for (int rowIndex = 0; rowIndex < optionsTable.getItemCount(); rowIndex++) {
            TableItem row = optionsTable.getTable().getItem(rowIndex);
            String config = row.getText(1);
            String value = row.getText(2);
            if (!StringUtils.isBlank(config) && !advancedConfig.containsKey(config)) {
                advancedConfig.put(config, value);
            }
        }
        meta.setConfig(advancedConfig);
    }
}
