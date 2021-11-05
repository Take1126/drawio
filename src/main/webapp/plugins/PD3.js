/*---------------------

グローバル変数の宣言

---------------------*/

//左サイドバー用
var arrow_inout_len = 100,
    arrow_updown_len = 80,
    box_width = 120,
    box_height = 60,
    object_width = 75,
    object_height = 45,
    startendbox_width = 60,
    startendbox_height = 60,
    container_height = 200,
    containerforEC_padding = 40,
    containerforEC_width = box_width * 7 + arrow_inout_len * 8 + containerforEC_padding * 2,
    ECDP_val = "Define Problem",
    ECDP_style = 'rounded=0;whiteSpace=wrap;html=1;fontSize=14;pd3layer=topic;pd3type=action;pd3action=ECDP;strokeColor=#d79b00;fontColor=#ffffff;fillColor=#D1BC35;',
    ECCAI_val = "Collect/Analyze Information",
    ECCAI_style = 'rounded=0;whiteSpace=wrap;html=1;fontSize=14;pd3layer=topic;pd3type=action;pd3action=ECCAI;strokeColor=#d79b00;fontColor=#ffffff;fillColor=#3E54E6;',
    ECGH_val = "Generate Hypothesis",
    ECGH_style = 'rounded=0;whiteSpace=wrap;html=1;fontSize=14;pd3layer=topic;pd3type=action;pd3action=ECGH;strokeColor=#d79b00;fontColor=#ffffff;fillColor=#C93AC9;',
    ECESI_val = "Evaluate/Select Information",
    ECESI_style = 'rounded=0;whiteSpace=wrap;html=1;fontSize=14;pd3layer=topic;pd3type=action;pd3action=ECESI;strokeColor=#d79b00;fontColor=#ffffff;fillColor=#8F4132;',
    ECEX_val = "Execute",
    ECEX_style = 'rounded=0;whiteSpace=wrap;html=1;fontSize=14;pd3layer=topic;pd3type=action;pd3action=ECEX;strokeColor=#d79b00;fontColor=#ffffff;fillColor=#2EBAC9;';

//右サイドバー用
var EP_URI = "",
    EP_URI_prefix = "",
    EP_URI_div,
    EP_URI_prefix_div,
    textbox1,
    textbox2,
    root,
    URI_val,
    prefix_val,
    title_val,
    creator_val,
    description_val,
    identifier_val,
    root_style = [];

/*---------------------

ノードとアークの初期設定

---------------------*/
Graph.prototype.defaultEdgeStyle = {
    edgeStyle: "",
    rounded: "0",
    jettySize: "auto",
    orthogonalLoop: "0",
    verticalAlign: "bottom",
    endArrow: "block",
    fontSize: "14"
};
Graph.prototype.defaultVertexStyle = {
    fontSize: "14"
};

/*---------------------

左サイドバー

---------------------*/

//左サイドバーの初期化
Sidebar.prototype.init = function() {
    this.addSearchPalette(false);
    this.addProblemSolvingLayerPalette(true);
    this.addInformationLayerPalette(true);
    this.addPhysicalLayerPalette(true);
};

//問題解決レイヤー
Sidebar.prototype.addProblemSolvingLayerPalette = function(expand) {
    var sb = this;
    var fns = [
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=topic;pd3type=action;fillColor=#ffe6cc;strokeColor=#d79b00;",
            box_width,
            box_height,
            "Action",
            "Problem-Solving Action Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            ECDP_style,
            box_width,
            box_height,
            ECDP_val,
            "Problem Definition Type",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            ECCAI_style,
            box_width,
            box_height,
            ECCAI_val,
            "Information Collection/Analysis Type",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            ECGH_style,
            box_width,
            box_height,
            ECGH_val,
            "Hypothesis Generation Type",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            ECESI_style,
            box_width,
            box_height,
            ECESI_val,
            "Information Evaluation/Selection Type",
            null,
            null,
            "rect rectangle box"
        ),

        this.createVertexTemplateEntry(
            ECEX_style,
            box_width,
            box_height,
            ECEX_val,
            "Execution Type",
            null,
            null,
            "rect rectangle box"
        ),
        this.addEntry('Engineering Cycle', function() {
            var cell1 = new mxCell(ECDP_val, new mxGeometry(0, 0, box_width, box_height), ECDP_style);
            cell1.vertex = true;
            var cell2 = new mxCell(ECCAI_val, new mxGeometry(box_width + arrow_inout_len, 0, box_width, box_height), ECCAI_style);
            cell2.vertex = true;
            var cell3 = new mxCell(ECGH_val, new mxGeometry((box_width + arrow_inout_len) * 2, 0, box_width, box_height), ECGH_style);
            cell3.vertex = true;
            var cell4 = new mxCell(ECESI_val, new mxGeometry((box_width + arrow_inout_len) * 3, 0, box_width, box_height), ECESI_style);
            cell4.vertex = true;
            var cell5 = new mxCell(ECEX_val, new mxGeometry((box_width + arrow_inout_len) * 4, 0, box_width, box_height), ECEX_style);
            cell5.vertex = true;

            var edge1 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge1.geometry.setTerminalPoint(new mxPoint(-arrow_inout_len, box_height / 2), true);
            edge1.geometry.setTerminalPoint(new mxPoint(0, box_height / 2), false);
            edge1.geometry.relative = true;
            edge1.edge = true;

            var edge2 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=0.5;exitX=1;exitY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge2.geometry.setTerminalPoint(new mxPoint(box_width, box_height / 2), true);
            edge2.geometry.setTerminalPoint(new mxPoint(box_width + arrow_inout_len, box_height / 2), false);
            edge2.geometry.relative = true;
            edge2.edge = true;

            var edge3 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=0.5;exitX=1;exitY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge3.geometry.setTerminalPoint(new mxPoint(box_width + (box_width + arrow_inout_len), box_height / 2), true);
            edge3.geometry.setTerminalPoint(new mxPoint((box_width + arrow_inout_len) * 2, box_height / 2), false);
            edge3.geometry.relative = true;
            edge3.edge = true;

            var edge4 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=0.5;exitX=1;exitY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge4.geometry.setTerminalPoint(new mxPoint(box_width + (box_width + arrow_inout_len) * 2, box_height / 2), true);
            edge4.geometry.setTerminalPoint(new mxPoint((box_width + arrow_inout_len) * 3, box_height / 2), false);
            edge4.geometry.relative = true;
            edge4.edge = true;

            var edge5 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=0.5;exitX=1;exitY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge5.geometry.setTerminalPoint(new mxPoint(box_width + (box_width + arrow_inout_len) * 3, box_height / 2), true);
            edge5.geometry.setTerminalPoint(new mxPoint((box_width + arrow_inout_len) * 4, box_height / 2), false);
            edge5.geometry.relative = true;
            edge5.edge = true;

            var edge6 = new mxCell('Output', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;exitX=1;exitY=0.5;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge6.geometry.setTerminalPoint(new mxPoint(box_width + (box_width + arrow_inout_len) * 4, box_height / 2), true);
            edge6.geometry.setTerminalPoint(new mxPoint((box_width + arrow_inout_len) * 5, box_height / 2), false);
            edge6.geometry.relative = true;
            edge6.edge = true;

            cell1.insertEdge(edge1, false);
            cell1.insertEdge(edge2, true);
            cell2.insertEdge(edge2, false);
            cell2.insertEdge(edge3, true);
            cell3.insertEdge(edge3, false);
            cell3.insertEdge(edge4, true);
            cell4.insertEdge(edge4, false);
            cell4.insertEdge(edge5, true);
            cell5.insertEdge(edge5, false);
            cell5.insertEdge(edge6, true);

            return sb.createVertexTemplateFromCells([cell1, cell2, cell3, cell4, cell5, edge1, edge2, edge3, edge4, edge5, edge6], 1000, box_height, 'Engineering Cycle');
        }),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=topic;pd3type=action;pd3action=start;fillColor=#ffe6cc;strokeColor=#d79b00;",
            startendbox_width,
            startendbox_height,
            "Start",
            "Problem-Solving Start Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=topic;fillColor=#ffe6cc;pd3type=action;pd3action=end;strokeColor=#d79b00;",
            startendbox_width,
            startendbox_height,
            "End",
            "Problem-Solving End Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createEdgeTemplateEntry('html=1;verticalAlign=bottom;endArrow=block;rounded=0;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;',
            arrow_inout_len,
            0,
            'Output',
            'Information Arrow',
            null,
            'uml sequence message call invoke dispatch'
        ),
        this.addEntry('Intention', function() {
            var edge = new mxCell('Intention', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Intention Arrow');
        }),
        this.addEntry('Tool/Knowledge', function() {
            var edge = new mxCell('Tool/Knowledge', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge.geometry.setTerminalPoint(new mxPoint(0, arrow_updown_len), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Tool Arrow');
        }),
        this.addEntry('Rationale', function() {
            var edge = new mxCell('Rationale', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(-30, arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Rationale Arrow');
        }),
        this.addEntry('Annotation', function() {
            var edge = new mxCell('Annotation', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=topic;pd3type=arrow;fillColor=#ffe6cc;strokeColor=#d79b00;dashed=1;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(-30, arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Annotation Arrow');
        }),
        this.createVertexTemplateEntry('swimlane;pd3layer=topic;pd3type=container;containertype=specialization;fillColor=#ffe6cc;strokeColor=#d79b00;', 400, container_height, 'Label of Parent Action Box', 'Problem-Solving Container', null, null, 'container swimlane lane pool group'),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=topic;pd3type=object;strokeColor=#d79b00;dashed=1;fontSize=14;fillColor=none;strokeWidth=1;",
            object_width,
            object_height,
            "Object",
            "Object",
            null,
            null,
            "rect rectangle box"
        ),
    ];
    this.addPaletteFunctions("Problem-Solving Layer", "Problem-Solving Layer", null != expand ? expand : true, fns);
};

//情報レイヤ―
Sidebar.prototype.addInformationLayerPalette = function(expand) {
    var sb = this;
    var fns = [
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=info;pd3type=action;fillColor=#dae8fc;strokeColor=#6c8ebf;",
            box_width,
            box_height,
            "Action",
            "Infomation Operation Action Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=info;pd3type=action;pd3action=start;fillColor=#dae8fc;strokeColor=#6c8ebf;",
            startendbox_width,
            startendbox_height,
            "Start",
            "Information Operation Start Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=info;pd3type=action;pd3action=end;fillColor=#dae8fc;strokeColor=#6c8ebf;",
            startendbox_width,
            startendbox_height,
            "End",
            "Information Operation End Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createEdgeTemplateEntry('html=1;verticalAlign=bottom;endArrow=block;rounded=0;pd3layer=info;pd3type=arrow;fillColor=#dae8fc;strokeColor=#6c8ebf;', arrow_inout_len, 0, 'Output', 'Information Arrow', null, 'uml sequence message call invoke dispatch'),
        this.addEntry('Intention', function() {
            var edge = new mxCell('Intention', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=info;pd3type=arrow;fillColor=#dae8fc;strokeColor=#6c8ebf;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Intention Arrow');
        }),
        this.addEntry('Tool/Knowledge', function() {
            var edge = new mxCell('Tool/Knowledge', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=info;pd3type=arrow;fillColor=#dae8fc;strokeColor=#6c8ebf;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, -arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;

            return sb.createEdgeTemplateFromCells([edge], 0, -arrow_updown_len, 'Tool Arrow');
        }),
        this.createVertexTemplateEntry('swimlane;pd3layer=info;pd3type=container;containertype=specialization;fillColor=#dae8fc;strokeColor=#6c8ebf;', 400, container_height, 'Label of Parent Action Box', 'Information Operation Container', null, null, 'container swimlane lane pool group'),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=info;pd3type=object;strokeColor=#6c8ebf;dashed=1;fontSize=14;fillColor=none;strokeWidth=1;",
            object_width,
            object_height,
            "Object",
            "Object",
            null,
            null,
            "rect rectangle box"
        ),

    ];
    this.addPaletteFunctions("Information Layer", "Information Layer", null != expand ? expand : true, fns);
};

//物理レイヤー
Sidebar.prototype.addPhysicalLayerPalette = function(expand) {
    var sb = this;
    var fns = [
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=phys;pd3type=action;fillColor=#d5e8d4;strokeColor=#82b366;",
            box_width,
            box_height,
            "Action",
            "Physical Operation Action Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=phys;pd3type=action;pd3action=start;fillColor=#d5e8d4;strokeColor=#82b366;",
            startendbox_width,
            startendbox_height,
            "Start",
            "Physical Operation Start Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;dashed=1;pd3layer=phys;pd3type=action;pd3action=end;fillColor=#d5e8d4;strokeColor=#82b366;",
            startendbox_width,
            startendbox_height,
            "End",
            "Physical Operation End Box",
            null,
            null,
            "rect rectangle box"
        ),
        this.createEdgeTemplateEntry('html=1;verticalAlign=bottom;endArrow=block;rounded=0;pd3layer=phys;pd3type=arrow;fillColor=#d5e8d4;strokeColor=#82b366;', arrow_inout_len, 0, 'Output', 'Information Arrow', null, 'uml sequence message call invoke dispatch'),

        this.addEntry('Intention', function() {
            var edge = new mxCell('Intention', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=phys;pd3type=arrow;fillColor=#d5e8d4;strokeColor=#82b366;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;
            return sb.createEdgeTemplateFromCells([edge], 0, arrow_updown_len, 'Intention Arrow');
        }),
        this.addEntry('Tool/Knowledge', function() {
            var edge = new mxCell('Tool/Knowledge', new mxGeometry(0, 0, 0, 0), 'endArrow=block;rounded=0;endFill=1;html=1;align=left;verticalAlign=middle;pd3layer=phys;pd3type=arrow;fillColor=#d5e8d4;strokeColor=#82b366;');
            edge.geometry.setTerminalPoint(new mxPoint(0, 0), true);
            edge.geometry.setTerminalPoint(new mxPoint(0, -arrow_updown_len), false);
            edge.geometry.relative = true;
            edge.edge = true;

            return sb.createEdgeTemplateFromCells([edge], 0, -arrow_updown_len, 'Tool Arrow');
        }),
        this.createVertexTemplateEntry('swimlane;pd3layer=phys;pd3type=container;containertype=specialization;fillColor=#d5e8d4;strokeColor=#82b366;', 400, container_height, 'Label of Parent Action Box', 'Physical Operation Container', null, null, 'container swimlane lane pool group'),
        this.createVertexTemplateEntry(
            "rounded=0;whiteSpace=wrap;html=1;pd3layer=phys;pd3type=object;strokeColor=#82b366;dashed=1;fontSize=14;fillColor=none;",
            object_width,
            object_height,
            "Object",
            "Object",
            null,
            null,
            "rect rectangle box"
        ),

    ];
    this.addPaletteFunctions("Physical Layer", "Physical Layer", null != expand ? expand : true, fns);
};


/*---------------------

XMLファイル出力時の非圧縮設定

---------------------*/
var menusInit = Menus.prototype.init;
Menus.prototype.init = function() {
    menusInit.apply(this, arguments);
    var editorUi = this.editorUi;
    var graph = editorUi.editor.graph;

    editorUi.actions.put('exportXml', new Action(mxResources.get('formatXml') + '...', function() {
        var div = document.createElement('div');
        div.style.whiteSpace = 'nowrap';
        var noPages = editorUi.pages == null || editorUi.pages.length <= 1;

        var hd = document.createElement('h3');
        mxUtils.write(hd, mxResources.get('formatXml'));
        hd.style.cssText = 'width:100%;text-align:center;margin-top:0px;margin-bottom:4px';
        div.appendChild(hd);

        var selection = editorUi.addCheckbox(div, mxResources.get('selectionOnly'),
            false, graph.isSelectionEmpty());
        //compressed　を　false　に変更
        var compressed = editorUi.addCheckbox(div, mxResources.get('compressed'), false);
        var pages = editorUi.addCheckbox(div, mxResources.get('allPages'), !noPages, noPages);
        pages.style.marginBottom = '16px';

        mxEvent.addListener(selection, 'change', function() {
            if (selection.checked) {
                pages.setAttribute('disabled', 'disabled');
            } else {
                pages.removeAttribute('disabled');
            }
        });

        var dlg = new CustomDialog(editorUi, div, mxUtils.bind(this, function() {
            editorUi.downloadFile('xml', !compressed.checked, null, !selection.checked, noPages || !pages.checked);
        }), null, mxResources.get('export'));

        editorUi.showDialog(dlg.container, 300, 180, true, true);
    }));
}

/*---------------------

ノードにマウスをホバーした時の設定

---------------------*/
//ノードから出力する矢印のレイヤーを，出力元のコンテナと同じにする
/*****setting for the graph made through hover icon, partycularly connection edge start*****/
Graph.prototype.isCloneConnectSource = function(source) {
    var layout = null;

    if (this.layoutManager != null) {
        layout = this.layoutManager.getLayout(this.model.getParent(source));
    }

    return this.isTableRow(source) || this.isTableCell(source) ||
        (layout != null && layout.constructor == mxStackLayout);
};

/**
 * Adds a connection to the given vertex.
 */
Graph.prototype.connectVertex = function(source, direction, length, evt, forceClone, ignoreCellAt, createTarget, done) {
    ignoreCellAt = (ignoreCellAt) ? ignoreCellAt : false;

    // Ignores relative edge labels
    if (source.geometry.relative && this.model.isEdge(source.parent)) {
        return [];
    }

    // Uses parent for relative child cells
    while (source.geometry.relative && this.model.isVertex(source.parent)) {
        source = source.parent;
    }

    // Handles clone connect sources
    var cloneSource = this.isCloneConnectSource(source);
    var composite = (cloneSource) ? source : this.getCompositeParent(source);

    var pt = (source.geometry.relative && source.parent.geometry != null) ?
        new mxPoint(source.parent.geometry.width * source.geometry.x,
            source.parent.geometry.height * source.geometry.y) :
        new mxPoint(composite.geometry.x, composite.geometry.y);

    if (direction == mxConstants.DIRECTION_NORTH) {
        pt.x += composite.geometry.width / 2;
        pt.y -= length;
    } else if (direction == mxConstants.DIRECTION_SOUTH) {
        pt.x += composite.geometry.width / 2;
        pt.y += composite.geometry.height + length;
    } else if (direction == mxConstants.DIRECTION_WEST) {
        pt.x -= length;
        pt.y += composite.geometry.height / 2;
    } else {
        pt.x += composite.geometry.width + length;
        pt.y += composite.geometry.height / 2;
    }

    var parentState = this.view.getState(this.model.getParent(source));
    var s = this.view.scale;
    var t = this.view.translate;
    var dx = t.x * s;
    var dy = t.y * s;

    if (parentState != null && this.model.isVertex(parentState.cell)) {
        dx = parentState.x;
        dy = parentState.y;
    }

    // Workaround for relative child cells
    if (this.model.isVertex(source.parent) && source.geometry.relative) {
        pt.x += source.parent.geometry.x;
        pt.y += source.parent.geometry.y;
    }

    // Checks actual end point of edge for target cell
    var rect = (!ignoreCellAt) ? new mxRectangle(dx + pt.x * s, dy + pt.y * s).grow(40) : null;
    var tempCells = (rect != null) ? this.getCells(0, 0, 0, 0, null, null, rect) : null;
    var target = (tempCells != null && tempCells.length > 0) ? tempCells.reverse()[0] : null;
    var keepParent = false;

    if (target != null && this.model.isAncestor(target, source)) {
        keepParent = true;
        target = null;
    }

    // Checks for swimlane at drop location
    if (target == null) {
        var temp = this.getSwimlaneAt(dx + pt.x * s, dy + pt.y * s);

        if (temp != null) {
            keepParent = false;
            target = temp;
        }
    }

    // Checks if target or ancestor is locked
    var temp = target;

    while (temp != null) {
        if (this.isCellLocked(temp)) {
            target = null;
            break;
        }

        temp = this.model.getParent(temp);
    }

    // Checks if source and target intersect
    if (target != null) {
        var sourceState = this.view.getState(source);
        var targetState = this.view.getState(target);

        if (sourceState != null && targetState != null && mxUtils.intersects(sourceState, targetState)) {
            target = null;
        }
    }

    var duplicate = (!mxEvent.isShiftDown(evt) || mxEvent.isControlDown(evt)) || forceClone;

    if (duplicate) {
        if (direction == mxConstants.DIRECTION_NORTH) {
            pt.y -= source.geometry.height / 2;
        } else if (direction == mxConstants.DIRECTION_SOUTH) {
            pt.y += source.geometry.height / 2;
        } else if (direction == mxConstants.DIRECTION_WEST) {
            pt.x -= source.geometry.width / 2;
        } else {
            pt.x += source.geometry.width / 2;
        }
    }

    // Uses connectable parent vertex if one exists
    // TODO: Fix using target as parent for swimlane
    if (target != null && !this.isCellConnectable(target) && !this.isSwimlane(target)) {
        var parent = this.getModel().getParent(target);

        if (this.getModel().isVertex(parent) && this.isCellConnectable(parent)) {
            target = parent;
        }
    }

    if (target == source || this.model.isEdge(target) ||
        !this.isCellConnectable(target) &&
        !this.isSwimlane(target)) {
        target = null;
    }

    var result = [];
    var swimlane = target != null && this.isSwimlane(target);
    var realTarget = (!swimlane) ? target : null;

    var execute = mxUtils.bind(this, function(targetCell) {
        if (createTarget == null || targetCell != null || (target == null && cloneSource)) {
            this.model.beginUpdate();
            try {
                if (realTarget == null && duplicate) {
                    // Handles relative children
                    var cellToClone = (targetCell != null) ? targetCell : source;
                    var geo = this.getCellGeometry(cellToClone);

                    while (geo != null && geo.relative) {
                        cellToClone = this.getModel().getParent(cellToClone);
                        geo = this.getCellGeometry(cellToClone);
                    }

                    // Handles composite cells for cloning
                    cellToClone = (cloneSource) ? source : this.getCompositeParent(cellToClone);
                    realTarget = (targetCell != null) ? targetCell : this.duplicateCells([cellToClone], false)[0];

                    if (targetCell != null) {
                        this.addCells([realTarget], this.model.getParent(source), null, null, null, true);
                    }

                    var geo = this.getCellGeometry(realTarget);

                    if (geo != null) {
                        geo.x = pt.x - geo.width / 2;
                        geo.y = pt.y - geo.height / 2;
                    }

                    if (swimlane) {
                        this.addCells([realTarget], target, null, null, null, true);
                        target = null;
                    } else if (duplicate && target == null && !keepParent && !cloneSource) {
                        this.addCells([realTarget], this.getDefaultParent(), null, null, null, true);
                    }
                }

                var edge = ((mxEvent.isControlDown(evt) && mxEvent.isShiftDown(evt) && duplicate) ||
                    (target == null && cloneSource)) ? null : this.insertEdge(this.model.getParent(source),
                    null, '', source, realTarget, this.createCurrentEdgeStyle());

                //修正個所　ここから
                var style = [];
                style = realTarget.style.split(';');
                var edgestyle = "";
                for (var i = 0; i < style.length; i++) {
                    if (style[i].indexOf('pd3layer') !== -1 || style[i].indexOf('fillColor') !== -1 || style[i].indexOf('strokeColor') !== -1) {
                        edgestyle = edgestyle + style[i] + ";";
                    }
                }
                edgestyle = edgestyle + "pd3type=arrow";
                edge.style = edge.style + edgestyle;
                //修正個所　ここまで


                // Inserts edge before source
                if (edge != null && this.connectionHandler.insertBeforeSource) {
                    var index = null;
                    var tmp = source;

                    while (tmp.parent != null && tmp.geometry != null &&
                        tmp.geometry.relative && tmp.parent != edge.parent) {
                        tmp = this.model.getParent(tmp);
                    }

                    if (tmp != null && tmp.parent != null && tmp.parent == edge.parent) {
                        var index = tmp.parent.getIndex(tmp);
                        this.model.add(tmp.parent, edge, index);
                    }
                }

                // Special case: Click on west icon puts clone before cell
                if (target == null && realTarget != null && source.parent != null &&
                    cloneSource && direction == mxConstants.DIRECTION_WEST) {
                    var index = source.parent.getIndex(source);
                    this.model.add(source.parent, realTarget, index);
                }

                if (edge != null) {
                    result.push(edge);
                }

                if (target == null && realTarget != null) {
                    result.push(realTarget);
                }

                if (realTarget == null && edge != null) {
                    edge.geometry.setTerminalPoint(pt, false);
                }

                if (edge != null) {
                    this.fireEvent(new mxEventObject('cellsInserted', 'cells', [edge]));
                }
            } finally {
                this.model.endUpdate();
            }
        }

        if (done != null) {
            done(result);
        } else {
            return result;
        }
    });

    if (createTarget != null && realTarget == null && duplicate &&
        (target != null || !cloneSource)) {
        createTarget(dx + pt.x * s, dy + pt.y * s, execute);
    } else {
        return execute(realTarget);
    }
};

//
HoverIcons.prototype.drag = function(evt, x, y) {
    this.graph.currentEdgeStyle.pd3type = '';
    this.graph.currentEdgeStyle.pd3layer = '';
    this.graph.popupMenuHandler.hideMenu();
    this.graph.stopEditing(false);

    // Checks if state was removed in call to stopEditing above
    if (this.currentState != null) {
        this.graph.connectionHandler.start(this.currentState, x, y);
        this.graph.isMouseTrigger = mxEvent.isMouseEvent(evt);
        this.graph.isMouseDown = true;

        var handler = this.graph.selectionCellsHandler.getHandler(this.currentState.cell);

        var style = [];
        var pd3layer_source = "",
            fillColor_source = "",
            strokeColor_source = "";
        style = this.currentState.cell.style.split(';');
        for (var i = 0; i < style.length; i++) {
            if (style[i].indexOf('pd3layer') !== -1) {
                pd3layer_source = style[i].replace('pd3layer=', '');
            } else if (style[i].indexOf('fillColor') !== -1) {
                fillColor_source = style[i].replace('fillColor=', '');
            } else if (style[i].indexOf('strokeColor') !== -1) {
                strokeColor_source = style[i].replace('strokeColor=', '');
            }
        }

        // Ctrl+shift drag sets source constraint
        var es = this.graph.connectionHandler.edgeState;

        this.graph.currentEdgeStyle.pd3layer = pd3layer_source;
        this.graph.currentEdgeStyle.fillColor = fillColor_source;
        this.graph.currentEdgeStyle.strokeColor = strokeColor_source;
        this.graph.currentEdgeStyle.pd3type = "arrow"

        es.style.pd3layer = pd3layer_source;
        es.style.fillColor = fillColor_source;
        es.style.strokeColor = strokeColor_source;
        es.style.pd3type = "arrow";

        es.cell.style = es.cell.style + "pd3layer=" + pd3layer_source + ";" + "pd3type=arrow;" + "fillColor=" + fillColor_source + ";" + "strokeColor=" + strokeColor_source + ";";

        if (handler != null) {
            handler.setHandlesVisible(false);
        }

        if (evt != null && mxEvent.isShiftDown(evt) && mxEvent.isControlDown(evt) && es != null &&
            mxUtils.getValue(es.style, mxConstants.STYLE_EDGE, null) === 'orthogonalEdgeStyle') {
            var direction = this.getDirection();
            es.cell.style = mxUtils.setStyle(es.cell.style, 'sourcePortConstraint', direction);
            es.style['sourcePortConstraint'] = direction;
        }

    }
};
/*****setting for the graph made through hover icon, partycularly connection edge end*****/
mxConnectionHandler.prototype.mouseDown = function(a, b) {
    this.mouseDownCounter++;
    if (this.isEnabled() && this.graph.isEnabled() && !b.isConsumed() && !this.isConnecting() && this.isStartEvent(b)) {
        null != this.constraintHandler.currentConstraint && null != this.constraintHandler.currentFocus && null != this.constraintHandler.currentPoint ? (this.sourceConstraint = this.constraintHandler.currentConstraint, this.previous = this.constraintHandler.currentFocus, this.first = this.constraintHandler.currentPoint.clone()) : this.first = new mxPoint(b.getGraphX(), b.getGraphY());
        var pd3layer_source = "",
            fillColor_source = "",
            strokeColor_source = "";
        pd3layer_source = b.sourceState.style['pd3layer'];
        fillColor_source = b.sourceState.style['fillColor'];
        strokeColor_source = b.sourceState.style['strokeColor'];
        this.edgeState = this.createEdgeState(b);
        this.graph.connectionHandler.edgeState['style'].pd3layer = pd3layer_source;
        this.graph.connectionHandler.edgeState['style'].fillColor = fillColor_source;
        this.graph.connectionHandler.edgeState['style'].strokeColor = strokeColor_source;
        this.graph.currentEdgeStyle.pd3layer = pd3layer_source;
        this.graph.currentEdgeStyle.fillColor = fillColor_source;
        this.graph.currentEdgeStyle.strokeColor = strokeColor_source;
        this.mouseDownCounter = 1;
        this.waypointsEnabled && null == this.shape && (this.waypoints = null, this.shape = this.createShape(), null != this.edgeState && this.shape.apply(this.edgeState));
        if (null == this.previous && null != this.edgeState) {
            var c = this.graph.getPointForEvent(b.getEvent());
            this.edgeState.cell.geometry.setTerminalPoint(c, !0)
        }
        this.fireEvent(new mxEventObject(mxEvent.START, "state", this.previous));
        b.consume()
    }
    this.selectedIcon = this.icon;
    this.icon = null
};



//2 functoins 
// ・set container in the same layer
// ・set parent process in the same layer
EditorUi.prototype.updateActionStates = function() {
    var a = this.editor.graph,
        c = !a.isSelectionEmpty(),
        d = !1,
        b = !1,
        f = !1,
        e = a.getSelectionCells();
    if (null != e)
        for (var h = 0; h < e.length; h++) {
            var g = e[h];
            a.getModel().isEdge(g) && (f = !0);
            a.getModel().isVertex(g) && (d = !0, 0 < a.getModel().getChildCount(g) || a.isContainer(g)) && (b = !0);
            if (f && d) break
        }
    e = "cut copy bold italic underline delete duplicate editStyle editTooltip editLink backgroundColor borderColor edit toFront toBack lockUnlock solid dashed pasteSize dotted fillColor gradientColor shadow fontColor formattedText rounded toggleRounded sharp strokeColor".split(" ");
    for (h = 0; h < e.length; h++) this.actions.get(e[h]).setEnabled(c);
    this.actions.get("setAsDefaultStyle").setEnabled(1 == a.getSelectionCount());
    this.actions.get("setProblemSolvingContainer").setEnabled(1 == a.getSelectionCount());
    this.actions.get("setInformationOperationContainer").setEnabled(1 == a.getSelectionCount());
    this.actions.get("setPhysicalOperationContainer").setEnabled(1 == a.getSelectionCount());
    this.actions.get("callPython").setEnabled(1 == a.getSelectionCount());
    this.actions.get("clearWaypoints").setEnabled(!a.isSelectionEmpty());
    this.actions.get("copySize").setEnabled(1 == a.getSelectionCount());
    this.actions.get("turn").setEnabled(!a.isSelectionEmpty());
    this.actions.get("curved").setEnabled(f);
    this.actions.get("rotation").setEnabled(d);
    this.actions.get("wordWrap").setEnabled(d);
    this.actions.get("autosize").setEnabled(d);
    f = d && 1 == a.getSelectionCount();
    this.actions.get("group").setEnabled(1 < a.getSelectionCount() || f && !a.isContainer(a.getSelectionCell()));
    this.actions.get("ungroup").setEnabled(b);
    this.actions.get("removeFromGroup").setEnabled(f && a.getModel().isVertex(a.getModel().getParent(a.getSelectionCell())));
    a.view.getState(a.getSelectionCell());
    this.menus.get("navigation").setEnabled(c || null != a.view.currentRoot);
    this.actions.get("collapsible").setEnabled(d && (a.isContainer(a.getSelectionCell()) || 0 < a.model.getChildCount(a.getSelectionCell())));
    this.actions.get("home").setEnabled(null != a.view.currentRoot);
    this.actions.get("exitGroup").setEnabled(null != a.view.currentRoot);
    this.actions.get("enterGroup").setEnabled(1 == a.getSelectionCount() && a.isValidRoot(a.getSelectionCell()));
    c = 1 == a.getSelectionCount() && a.isCellFoldable(a.getSelectionCell());
    this.actions.get("expand").setEnabled(c);
    this.actions.get("collapse").setEnabled(c);
    this.actions.get("editLink").setEnabled(1 == a.getSelectionCount());
    this.actions.get("openLink").setEnabled(1 == a.getSelectionCount() && null != a.getLinkForCell(a.getSelectionCell()));
    this.actions.get("guides").setEnabled(a.isEnabled());
    this.actions.get("grid").setEnabled(!this.editor.chromeless || this.editor.editable);
    c = a.isEnabled() && !a.isCellLocked(a.getDefaultParent());
    this.menus.get("layout").setEnabled(c);
    this.menus.get("insert").setEnabled(c);
    this.menus.get("direction").setEnabled(c && d);
    this.menus.get("align").setEnabled(c && d && 1 < a.getSelectionCount());
    this.menus.get("distribute").setEnabled(c && d && 1 < a.getSelectionCount());
    this.actions.get("selectVertices").setEnabled(c);
    this.actions.get("selectEdges").setEnabled(c);
    this.actions.get("selectAll").setEnabled(c);
    this.actions.get("selectNone").setEnabled(c);
    this.updatePasteActionStates()
};

EditorUi.prototype.createKeyHandler = function(a) {
    function c(a, c, d) {
        h.push(function() {
            if (!b.isSelectionEmpty() && b.isEnabled())
                if (c = null != c ? c : 1, d) {
                    b.getModel().beginUpdate();
                    try {
                        for (var e = b.getSelectionCells(), f = 0; f < e.length; f++)
                            if (b.getModel().isVertex(e[f]) && b.isCellResizable(e[f])) {
                                var g = b.getCellGeometry(e[f]);
                                null != g && (g = g.clone(), 37 == a ? g.width = Math.max(0, g.width - c) : 38 == a ? g.height = Math.max(0, g.height - c) : 39 == a ? g.width += c : 40 == a && (g.height += c), b.getModel().setGeometry(e[f], g))
                            }
                    } finally {
                        b.getModel().endUpdate()
                    }
                } else {
                    var h = b.getSelectionCell(),
                        g = b.model.getParent(h),
                        e = null;
                    1 == b.getSelectionCount() && b.model.isVertex(h) && null != b.layoutManager && !b.isCellLocked(h) && (e = b.layoutManager.getLayout(g));
                    if (null != e && e.constructor == mxStackLayout) e = g.getIndex(h), 37 == a || 38 == a ? b.model.add(g, h, Math.max(0, e - 1)) : 39 != a && 40 != a || b.model.add(g, h, Math.min(b.model.getChildCount(g), e + 1));
                    else {
                        e = b.getMovableCells(b.getSelectionCells());
                        h = [];
                        for (f = 0; f < e.length; f++) g = b.getCurrentCellStyle(e[f]), "1" == mxUtils.getValue(g, "part", "0") ? (g = b.model.getParent(e[f]), b.model.isVertex(g) && 0 > mxUtils.indexOf(e, g) && h.push(g)) : h.push(e[f]);
                        0 < h.length && (e = g = 0, 37 == a ? g = -c : 38 == a ? e = -c : 39 == a ? g = c : 40 == a && (e = c), b.moveCells(h, g, e))
                    }
                }
        });
        null != g && window.clearTimeout(g);
        g = window.setTimeout(function() {
            if (0 < h.length) {
                b.getModel().beginUpdate();
                try {
                    for (var a = 0; a < h.length; a++) h[a]();
                    h = []
                } finally {
                    b.getModel().endUpdate()
                }
            }
        }, 200)
    }
    var d = this,
        b = this.editor.graph,
        f = new mxKeyHandler(b),
        e = f.isEventIgnored;
    f.isEventIgnored = function(a) {
        return !(mxEvent.isShiftDown(a) && 9 == a.keyCode) && (!this.isControlDown(a) || mxEvent.isShiftDown(a) || 90 != a.keyCode && 89 != a.keyCode && 188 != a.keyCode && 190 != a.keyCode && 85 != a.keyCode) && (66 != a.keyCode && 73 != a.keyCode || !this.isControlDown(a) || this.graph.cellEditor.isContentEditing() && !mxClient.IS_FF && !mxClient.IS_SF) && e.apply(this, arguments)
    };
    f.isEnabledForEvent = function(a) {
        return !mxEvent.isConsumed(a) && this.isGraphEvent(a) && this.isEnabled() && (null == d.dialogs || 0 == d.dialogs.length)
    };
    f.isControlDown = function(a) {
        return mxEvent.isControlDown(a) || mxClient.IS_MAC && a.metaKey
    };
    var h = [],
        g = null,
        k = {
            37: mxConstants.DIRECTION_WEST,
            38: mxConstants.DIRECTION_NORTH,
            39: mxConstants.DIRECTION_EAST,
            40: mxConstants.DIRECTION_SOUTH
        },
        l = f.getFunction;
    mxKeyHandler.prototype.getFunction = function(a) {
        if (b.isEnabled()) {
            if (mxEvent.isShiftDown(a) && mxEvent.isAltDown(a)) {
                var e = d.actions.get(d.altShiftActions[a.keyCode]);
                if (null != e) return e.funct
            }
            if (9 == a.keyCode && mxEvent.isAltDown(a)) return b.cellEditor.isContentEditing() ? function() {
                document.execCommand("outdent", !1, null)
            } : mxEvent.isShiftDown(a) ? function() {
                b.selectParentCell()
            } : function() {
                b.selectChildCell()
            };
            if (null != k[a.keyCode] && !b.isSelectionEmpty())
                if (!this.isControlDown(a) && mxEvent.isShiftDown(a) && mxEvent.isAltDown(a)) {
                    if (b.model.isVertex(b.getSelectionCell())) return function() {
                        var c = b.connectVertex(b.getSelectionCell(), k[a.keyCode], b.defaultEdgeLength, a, !0);
                        null != c && 0 < c.length && (1 == c.length && b.model.isEdge(c[0]) ? b.setSelectionCell(b.model.getTerminal(c[0], !1)) : b.setSelectionCell(c[c.length - 1]), b.scrollCellToVisible(b.getSelectionCell()), null != d.hoverIcons && d.hoverIcons.update(b.view.getState(b.getSelectionCell())))
                    }
                } else return this.isControlDown(a) ? function() {
                    c(a.keyCode, mxEvent.isShiftDown(a) ? b.gridSize : null, !0)
                } : function() {
                    c(a.keyCode, mxEvent.isShiftDown(a) ? b.gridSize : null)
                }
        }
        return l.apply(this, arguments)
    };
    f.bindAction = mxUtils.bind(this, function(a, b, c, d) {
        var e = this.actions.get(c);
        null != e && (c = function() {
            e.isEnabled() && e.funct()
        }, b ? d ? f.bindControlShiftKey(a, c) : f.bindControlKey(a, c) : d ? f.bindShiftKey(a, c) : f.bindKey(a, c))
    });
    var n = this,
        m = f.escape;
    f.escape = function(a) {
        m.apply(this, arguments)
    };
    f.enter = function() {};
    f.bindControlShiftKey(36, function() {
        b.exitGroup()
    });
    f.bindControlShiftKey(35, function() {
        b.enterGroup()
    });
    f.bindShiftKey(36, function() {
        b.home()
    });
    f.bindKey(35, function() {
        b.refresh()
    });
    f.bindAction(107, !0, "zoomIn");
    f.bindAction(109, !0, "zoomOut");
    f.bindAction(80, !0, "print");
    f.bindAction(79, !0, "outline", !0);
    if (!this.editor.chromeless || this.editor.editable) f.bindControlKey(36, function() {
            b.isEnabled() && b.foldCells(!0)
        }), f.bindControlKey(35, function() {
            b.isEnabled() && b.foldCells(!1)
        }), f.bindControlKey(13, function() {
            n.ctrlEnter()
        }), f.bindAction(8, !1, "delete"),
        f.bindAction(8, !0, "deleteAll"), f.bindAction(46, !1, "delete"), f.bindAction(46, !0, "deleteAll"), f.bindAction(36, !1, "resetView"), f.bindAction(72, !0, "fitWindow", !0), f.bindAction(74, !0, "fitPage"), f.bindAction(74, !0, "fitTwoPages", !0), f.bindAction(48, !0, "customZoom"), f.bindAction(82, !0, "turn"), f.bindAction(82, !0, "clearDefaultStyle", !0), f.bindAction(83, !0, "save"), f.bindAction(83, !0, "saveAs", !0), f.bindAction(65, !0, "selectAll"), f.bindAction(65, !0, "selectNone", !0), f.bindAction(73, !0, "selectVertices", !0), f.bindAction(69, !0, "selectEdges", !0), f.bindAction(69, !0, "editStyle"), f.bindAction(66, !0, "bold"), f.bindAction(66, !0, "toBack", !0), f.bindAction(70, !0, "toFront", !0), f.bindAction(68, !0, "duplicate"), f.bindAction(68, !0, "setAsDefaultStyle", !0), f.bindAction(90, !0, "undo"), f.bindAction(89, !0, "autosize", !0), f.bindAction(88, !0, "cut"), f.bindAction(67, !0, "copy"), f.bindAction(86, !0, "paste"), f.bindAction(71, !0, "group"), f.bindAction(77, !0, "editData"), f.bindAction(71, !0, "grid", !0), f.bindAction(73, !0, "italic"), f.bindAction(76, !0, "lockUnlock"),
        f.bindAction(76, !0, "layers", !0), f.bindAction(80, !0, "formatPanel", !0), f.bindAction(85, !0, "underline"), f.bindAction(85, !0, "ungroup", !0), f.bindAction(190, !0, "superscript"), f.bindAction(188, !0, "subscript"), f.bindAction(9, !1, "indent", !0), f.bindKey(13, function() {
            b.isEnabled() && b.startEditingAtCell()
        }), f.bindKey(113, function() {
            b.isEnabled() && b.startEditingAtCell()
        });
    mxClient.IS_WIN ? f.bindAction(89, !0, "redo") : f.bindAction(90, !0, "redo", !0);
    return f
};

Actions.prototype.init = function() {
    function a(a) {
        b.escape();
        a = b.deleteCells(b.getDeletableCells(b.getSelectionCells()), a);
        null != a && b.setSelectionCells(a)
    }
    var c = this.editorUi,
        d = c.editor,
        b = d.graph,
        f = function() {
            return Action.prototype.isEnabled.apply(this, arguments) && b.isEnabled()
        };
    this.addAction("new...", function() {
        b.openLink(c.getUrl())
    });
    this.addAction("open...", function() {
        window.openNew = !0;
        window.openKey = "open";
        c.openFile()
    });
    this.addAction("import...", function() {
        window.openNew = !1;
        window.openKey = "import";
        window.openFile = new OpenFile(mxUtils.bind(this, function() {
            c.hideDialog()
        }));
        window.openFile.setConsumer(mxUtils.bind(this, function(a, b) {
            try {
                var c = mxUtils.parseXml(a);
                d.graph.setSelectionCells(d.graph.importGraphModel(c.documentElement))
            } catch (n) {
                mxUtils.alert(mxResources.get("invalidOrMissingFile") + ": " + n.message)
            }
        }));
        c.showDialog((new OpenDialog(this)).container, 320, 220, !0, !0, function() {
            window.openFile = null
        })
    }).isEnabled = f;
    this.addAction("save", function() {
        c.saveFile(!1)
    }, null, null, Editor.ctrlKey + "+S").isEnabled = f;
    this.addAction("saveAs...", function() {
        c.saveFile(!0)
    }, null, null, Editor.ctrlKey + "+Shift+S").isEnabled = f;
    this.addAction("export...", function() {
        c.showDialog((new ExportDialog(c)).container, 300, 296, !0, !0)
    });
    this.addAction("editDiagram...", function() {
        var a = new EditDiagramDialog(c);
        c.showDialog(a.container, 620, 420, !0, !1);
        a.init()
    });
    this.addAction("pageSetup...", function() {
        c.showDialog((new PageSetupDialog(c)).container, 320, 220, !0, !0)
    }).isEnabled = f;
    this.addAction("print...", function() {
        c.showDialog((new PrintDialog(c)).container, 300, 180, !0, !0)
    }, null, "sprite-print", Editor.ctrlKey + "+P");
    this.addAction("preview", function() {
        mxUtils.show(b, null, 10, 10)
    });
    this.addAction("undo", function() {
        c.undo()
    }, null, "sprite-undo", Editor.ctrlKey + "+Z");
    this.addAction("redo", function() {
        c.redo()
    }, null, "sprite-redo", mxClient.IS_WIN ? Editor.ctrlKey + "+Y" : Editor.ctrlKey + "+Shift+Z");
    this.addAction("cut", function() {
        mxClipboard.cut(b)
    }, null, "sprite-cut", Editor.ctrlKey + "+X");
    this.addAction("copy", function() {
        try {
            mxClipboard.copy(b)
        } catch (g) {
            c.handleError(g)
        }
    }, null, "sprite-copy", Editor.ctrlKey + "+C");
    this.addAction("paste", function() {
        b.isEnabled() && !b.isCellLocked(b.getDefaultParent()) && mxClipboard.paste(b)
    }, !1, "sprite-paste", Editor.ctrlKey + "+V");
    this.addAction("pasteHere", function(a) {
        if (b.isEnabled() && !b.isCellLocked(b.getDefaultParent())) {
            b.getModel().beginUpdate();
            try {
                var c = mxClipboard.paste(b);
                if (null != c) {
                    a = !0;
                    for (var d = 0; d < c.length && a; d++) a = a && b.model.isEdge(c[d]);
                    var e = b.view.translate,
                        f = b.view.scale,
                        g = e.x,
                        h = e.y,
                        e = null;
                    if (1 == c.length && a) {
                        var t = b.getCellGeometry(c[0]);
                        null != t && (e = t.getTerminalPoint(!0))
                    }
                    e = null != e ? e : b.getBoundingBoxFromGeometry(c, a);
                    if (null != e) {
                        var v = Math.round(b.snap(b.popupMenuHandler.triggerX / f - g)),
                            u = Math.round(b.snap(b.popupMenuHandler.triggerY / f - h));
                        b.cellsMoved(c, v - e.x, u - e.y)
                    }
                }
            } finally {
                b.getModel().endUpdate()
            }
        }
    });
    this.addAction("copySize", function(a) {
        a = b.getSelectionCell();
        b.isEnabled() && null != a && b.getModel().isVertex(a) && (a = b.getCellGeometry(a), null != a && (c.copiedSize = new mxRectangle(a.x, a.y, a.width, a.height)))
    }, null, null, "Alt+Shift+X");
    this.addAction("pasteSize", function(a) {
        if (b.isEnabled() && !b.isSelectionEmpty() && null != c.copiedSize) {
            b.getModel().beginUpdate();
            try {
                var d = b.getSelectionCells();
                for (a = 0; a < d.length; a++)
                    if (b.getModel().isVertex(d[a])) {
                        var e = b.getCellGeometry(d[a]);
                        null != e && (e = e.clone(), e.width = c.copiedSize.width, e.height = c.copiedSize.height, b.getModel().setGeometry(d[a], e))
                    }
            } finally {
                b.getModel().endUpdate()
            }
        }
    }, null, null, "Alt+Shift+V");
    this.addAction("delete", function(b) {
        a(null != b && mxEvent.isShiftDown(b))
    }, null, null, "Delete");
    this.addAction("deleteAll", function() {
        a(!0)
    }, null, null, Editor.ctrlKey + "+Delete");
    this.addAction("duplicate", function() {
        try {
            b.setSelectionCells(b.duplicateCells()),
                b.scrollCellToVisible(b.getSelectionCell())
        } catch (g) {
            c.handleError(g)
        }
    }, null, null, Editor.ctrlKey + "+D");
    this.put("turn", new Action(mxResources.get("turn") + " / " + mxResources.get("reverse"), function(a) {
        b.turnShapes(b.getSelectionCells(), null != a ? mxEvent.isShiftDown(a) : !1)
    }, null, null, Editor.ctrlKey + "+R"));
    this.addAction("selectVertices", function() {
        b.selectVertices(null, !0)
    }, null, null, Editor.ctrlKey + "+Shift+I");
    this.addAction("selectEdges", function() {
        b.selectEdges()
    }, null, null, Editor.ctrlKey + "+Shift+E");
    this.addAction("selectAll", function() {
        b.selectAll(null, !0)
    }, null, null, Editor.ctrlKey + "+A");
    this.addAction("selectNone", function() {
        b.clearSelection()
    }, null, null, Editor.ctrlKey + "+Shift+A");
    this.addAction("lockUnlock", function() {
        if (!b.isSelectionEmpty()) {
            b.getModel().beginUpdate();
            try {
                var a = b.isCellMovable(b.getSelectionCell()) ? 1 : 0;
                b.toggleCellStyles(mxConstants.STYLE_MOVABLE, a);
                b.toggleCellStyles(mxConstants.STYLE_RESIZABLE, a);
                b.toggleCellStyles(mxConstants.STYLE_ROTATABLE, a);
                b.toggleCellStyles(mxConstants.STYLE_DELETABLE, a);
                b.toggleCellStyles(mxConstants.STYLE_EDITABLE, a);
                b.toggleCellStyles("connectable", a)
            } finally {
                b.getModel().endUpdate()
            }
        }
    }, null, null, Editor.ctrlKey + "+L");
    this.addAction("home", function() {
        b.home()
    }, null, null, "Shift+Home");
    this.addAction("exitGroup", function() {
        b.exitGroup()
    }, null, null, Editor.ctrlKey + "+Shift+Home");
    this.addAction("enterGroup", function() {
        b.enterGroup()
    }, null, null, Editor.ctrlKey + "+Shift+End");
    this.addAction("collapse", function() {
        b.foldCells(!0)
    }, null, null, Editor.ctrlKey + "+Home");
    this.addAction("expand", function() {
        b.foldCells(!1)
    }, null, null, Editor.ctrlKey + "+End");
    this.addAction("toFront", function() {
        b.orderCells(!1)
    }, null, null, Editor.ctrlKey + "+Shift+F");
    this.addAction("toBack", function() {
        b.orderCells(!0)
    }, null, null, Editor.ctrlKey + "+Shift+B");
    this.addAction("group", function() {
        1 == b.getSelectionCount() ? b.setCellStyles("container", "1") : b.setSelectionCell(b.groupCells(null, 0))
    }, null, null, Editor.ctrlKey + "+G");
    this.addAction("ungroup", function() {
        if (b.isEnabled() && !b.isSelectionEmpty()) {
            var a = b.getSelectionCells();
            b.model.beginUpdate();
            try {
                var c = b.ungroupCells();
                if (null != a)
                    for (var d = 0; d < a.length; d++) b.model.contains(a[d]) && (0 == b.model.getChildCount(a[d]) && b.model.isVertex(a[d]) && b.setCellStyles("container", "0", [a[d]]), c.push(a[d]))
            } finally {
                b.model.endUpdate()
            }
            b.setSelectionCells(c)
        }
    }, null, null, Editor.ctrlKey + "+Shift+U");
    this.addAction("removeFromGroup", function() {
        b.removeCellsFromParent()
    });
    this.addAction("edit", function() {
        b.isEnabled() && b.startEditingAtCell()
    }, null, null, "F2/Enter");
    this.addAction("editData...", function() {
        var a = b.getSelectionCell() || b.getModel().getRoot();
        c.showDataDialog(a)
    }, null, null, Editor.ctrlKey + "+M");

    this.addAction("editURI", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("URI" in root_style) {
                var URI_val = root_style["URI"];
            } else {
                URI_val = "";
            }
        } else {
            root_style = {};
            URI_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit URI :", URI_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["URI"] = input;
                mxUtils.setTextContent(EP_URI_div, "URI : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_div, "URI : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);


    this.addAction("editprefix", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("prefix" in root_style) {
                var prefix_val = root_style["prefix"];
            } else {
                prefix_val = "";
            }
        } else {
            root_style = {};
            prefix_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit prefix of URI :", prefix_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["prefix"] = input;
                mxUtils.setTextContent(EP_URI_prefix_div, "Prefix : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_prefix_div, "Prefix : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);

    this.addAction("edittitle", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("title" in root_style) {
                var title_val = root_style["title"];
            } else {
                title_val = "";
            }
        } else {
            root_style = {};
            title_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit title:", title_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["title"] = input;
                mxUtils.setTextContent(EP_URI_title_div, "title : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_title_div, "title : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);

    this.addAction("editcreator", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("creator" in root_style) {
                var creator_val = root_style["creator"];
            } else {
                creator_val = "";
            }
        } else {
            root_style = {};
            creator_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit creator:", creator_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["creator"] = input;
                mxUtils.setTextContent(EP_URI_creator_div, "creator : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_creator_div, "creator : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);

    this.addAction("editdescription", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("description" in root_style) {
                var description_val = root_style["description"];
            } else {
                description_val = "";
            }
        } else {
            root_style = {};
            description_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit description:", description_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["description"] = input;
                mxUtils.setTextContent(EP_URI_description_div, "description : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_description_div, "description : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);

    this.addAction("editidentifier", mxUtils.bind(this, function() {
        var graph = b;
        var graphModel = graph.getModel();
        root = graphModel.root;
        root_style = setStyleArray(root);
        if (root_style != null) {
            if ("identifier" in root_style) {
                var identifier_val = root_style["identifier"];
            } else {
                identifier_val = "";
            }
        } else {
            root_style = {};
            identifier_val = "";
        }
        graphModel = new TextareaDialog(this.editorUi, "Edit identifier:", identifier_val, function(graphModel) {
            var input = mxUtils.ltrim(graphModel);
            input = input.replace(/\r?\n/g, "");
            if (input != "") {
                root_style["identifier"] = input;
                mxUtils.setTextContent(EP_URI_identifier_div, "Identifier : " + input);
            } else {
                mxUtils.setTextContent(EP_URI_identifier_div, "Identifier : none");
            }

            root.style = setStyleString(root_style);
        }, null, null, 500, 30);
        this.editorUi.showDialog(graphModel.container, 520, 130, !0, !0);
        graphModel.init()
    }), null, null, null);

    this.addAction("editTooltip...", function() {
        if (b.isEnabled() && !b.isSelectionEmpty()) {
            var a = b.getSelectionCell(),
                d = "";
            if (mxUtils.isNode(a.value)) {
                var e = a.value.getAttribute("tooltip");
                null != e && (d = e)
            }
            d = new TextareaDialog(c, mxResources.get("editTooltip") + ":", d, function(c) {
                b.setTooltipForCell(a, c)
            });
            c.showDialog(d.container, 320, 200, !0, !0);
            d.init()
        }
    }, null, null, "Alt+Shift+T");
    this.addAction("openLink", function() {
        var a = b.getLinkForCell(b.getSelectionCell());
        null != a && b.openLink(a)
    });
    this.addAction("editLink...", function() {
        if (b.isEnabled() && !b.isSelectionEmpty()) {
            var a = b.getSelectionCell(),
                d = b.getLinkForCell(a) || "";
            c.showLinkDialog(d, mxResources.get("apply"), function(c) {
                c = mxUtils.trim(c);
                b.setLinkForCell(a, 0 < c.length ? c : null)
            })
        }
    }, null, null, "Alt+Shift+L");
    this.put("insertImage", new Action(mxResources.get("image") + "...", function() {
        b.isEnabled() && !b.isCellLocked(b.getDefaultParent()) && (b.clearSelection(), c.actions.get("image").funct())
    })).isEnabled = f;
    this.put("insertLink", new Action(mxResources.get("link") + "...", function() {
        b.isEnabled() && !b.isCellLocked(b.getDefaultParent()) && c.showLinkDialog("", mxResources.get("insert"), function(a, c) {
            a = mxUtils.trim(a);
            if (0 < a.length) {
                var d = null,
                    e = b.getLinkTitle(a);
                null != c && 0 < c.length && (d = c[0].iconUrl, e = c[0].name || c[0].type, e = e.charAt(0).toUpperCase() + e.substring(1), 30 < e.length && (e = e.substring(0, 30) + "..."));
                d = new mxCell(e, new mxGeometry(0, 0, 100, 40), "fontColor=#0000EE;fontStyle=4;rounded=1;overflow=hidden;" + (null != d ? "shape=label;imageWidth=16;imageHeight=16;spacingLeft=26;align=left;image=" + d : "spacing=10;"));
                d.vertex = !0;
                e = b.getCenterInsertPoint(b.getBoundingBoxFromGeometry([d], !0));
                d.geometry.x = e.x;
                d.geometry.y = e.y;
                b.setLinkForCell(d, a);
                b.cellSizeUpdated(d, !0);
                b.getModel().beginUpdate();
                try {
                    d = b.addCell(d), b.fireEvent(new mxEventObject("cellsInserted", "cells", [d]))
                } finally {
                    b.getModel().endUpdate()
                }
                b.setSelectionCell(d);
                b.scrollCellToVisible(b.getSelectionCell())
            }
        })
    })).isEnabled = f;
    this.addAction("link...", mxUtils.bind(this, function() {
        if (b.isEnabled())
            if (b.cellEditor.isContentEditing()) {
                var a = b.getSelectedElement(),
                    d = b.getParentByName(a, "A", b.cellEditor.textarea),
                    e = "";
                if (null == d && null != a && null != a.getElementsByTagName)
                    for (var f = a.getElementsByTagName("a"), h = 0; h < f.length && null == d; h++) f[h].textContent == a.textContent && (d = f[h]);
                null != d && "A" == d.nodeName && (e = d.getAttribute("href") || "", b.selectNode(d));
                var p = b.cellEditor.saveSelection();
                c.showLinkDialog(e, mxResources.get("apply"), mxUtils.bind(this, function(a) {
                    b.cellEditor.restoreSelection(p);
                    null != a && b.insertLink(a)
                }))
            } else b.isSelectionEmpty() ? this.get("insertLink").funct() : this.get("editLink").funct()
    })).isEnabled = f;
    this.addAction("autosize", function() {
        var a = b.getSelectionCells();
        if (null != a) {
            b.getModel().beginUpdate();
            try {
                for (var c = 0; c < a.length; c++) {
                    var d = a[c];
                    if (b.getModel().getChildCount(d)) b.updateGroupBounds([d], 20);
                    else {
                        var e = b.view.getState(d),
                            f = b.getCellGeometry(d);
                        b.getModel().isVertex(d) && null != e && null != e.text && null != f && b.isWrapping(d) ? (f = f.clone(), f.height = e.text.boundingBox.height / b.view.scale, b.getModel().setGeometry(d, f)) : b.updateCellSize(d)
                    }
                }
            } finally {
                b.getModel().endUpdate()
            }
        }
    }, null, null, Editor.ctrlKey + "+Shift+Y");
    this.addAction("formattedText", function() {
        var a = b.getView().getState(b.getSelectionCell());
        if (null != a) {
            b.stopEditing();
            a = "1" == a.style.html ? null : "1";
            b.getModel().beginUpdate();
            try {
                for (var d = b.getSelectionCells(), e = 0; e < d.length; e++)
                    if (state = b.getView().getState(d[e]), null != state) {
                        var f = mxUtils.getValue(state.style, "html", "0");
                        if ("1" == f && null == a) {
                            var h = b.convertValueToString(state.cell);
                            "0" != mxUtils.getValue(state.style, "nl2Br", "1") && (h = h.replace(/\n/g, "").replace(/<br\s*.?>/g, "\n"));
                            var p = document.createElement("div");
                            p.innerHTML = b.sanitizeHtml(h);
                            h = mxUtils.extractTextWithWhitespace(p.childNodes);
                            b.cellLabelChanged(state.cell, h);
                            b.setCellStyles("html", a, [d[e]])
                        } else "0" == f && "1" == a && (h = mxUtils.htmlEntities(b.convertValueToString(state.cell), !1), "0" != mxUtils.getValue(state.style, "nl2Br", "1") && (h = h.replace(/\n/g, "<br/>")), b.cellLabelChanged(state.cell, b.sanitizeHtml(h)), b.setCellStyles("html", a, [d[e]]))
                    }
                c.fireEvent(new mxEventObject("styleChanged", "keys", ["html"], "values", [null != a ? a : "0"], "cells", d))
            } finally {
                b.getModel().endUpdate()
            }
        }
    });
    this.addAction("wordWrap", function() {
        var a = b.getView().getState(b.getSelectionCell()),
            c = "wrap";
        b.stopEditing();
        null != a && "wrap" == a.style[mxConstants.STYLE_WHITE_SPACE] && (c = null);
        b.setCellStyles(mxConstants.STYLE_WHITE_SPACE, c)
    });
    this.addAction("rotation", function() {
        var a = "0",
            d = b.getView().getState(b.getSelectionCell());
        null != d && (a = d.style[mxConstants.STYLE_ROTATION] || a);
        a = new FilenameDialog(c, a, mxResources.get("apply"), function(a) {
            null != a && 0 < a.length && b.setCellStyles(mxConstants.STYLE_ROTATION, a)
        }, mxResources.get("enterValue") + " (" + mxResources.get("rotation") + " 0-360)");
        c.showDialog(a.container, 375, 80, !0, !0);
        a.init()
    });
    this.addAction("resetView", function() {
        b.zoomTo(1);
        c.resetScrollbars()
    }, null, null, "Home");
    this.addAction("zoomIn", function(a) {
        b.isFastZoomEnabled() ? b.lazyZoom(!0, !0, c.buttonZoomDelay) : b.zoomIn()
    }, null, null, Editor.ctrlKey + " + (Numpad) / Alt+Mousewheel");
    this.addAction("zoomOut", function(a) {
        b.isFastZoomEnabled() ? b.lazyZoom(!1, !0, c.buttonZoomDelay) : b.zoomOut()
    }, null, null, Editor.ctrlKey + " - (Numpad) / Alt+Mousewheel");
    this.addAction("fitWindow", function() {
        var a = b.isSelectionEmpty() ? b.getGraphBounds() : b.getBoundingBox(b.getSelectionCells()),
            d = b.view.translate,
            e = b.view.scale;
        a.x = a.x / e - d.x;
        a.y = a.y / e - d.y;
        a.width /= e;
        a.height /= e;
        null != b.backgroundImage && a.add(new mxRectangle(0, 0, b.backgroundImage.width, b.backgroundImage.height));
        0 == a.width || 0 == a.height ? (b.zoomTo(1), c.resetScrollbars()) : b.fitWindow(a)
    }, null, null, Editor.ctrlKey + "+Shift+H");
    this.addAction("fitPage", mxUtils.bind(this, function() {
        b.pageVisible || this.get("pageView").funct();
        var a = b.pageFormat,
            c = b.pageScale;
        b.zoomTo(Math.floor(20 * Math.min((b.container.clientWidth - 10) / a.width / c, (b.container.clientHeight - 10) / a.height / c)) / 20);
        mxUtils.hasScrollbars(b.container) && (a = b.getPagePadding(), b.container.scrollTop = a.y * b.view.scale - 1, b.container.scrollLeft = Math.min(a.x * b.view.scale, (b.container.scrollWidth - b.container.clientWidth) / 2) - 1)
    }), null, null, Editor.ctrlKey + "+J");
    this.addAction("fitTwoPages", mxUtils.bind(this, function() {
        b.pageVisible || this.get("pageView").funct();
        var a = b.pageFormat,
            c = b.pageScale;
        b.zoomTo(Math.floor(20 * Math.min((b.container.clientWidth - 10) / (2 * a.width) / c, (b.container.clientHeight - 10) / a.height / c)) / 20);
        mxUtils.hasScrollbars(b.container) && (a = b.getPagePadding(), b.container.scrollTop = Math.min(a.y, (b.container.scrollHeight - b.container.clientHeight) / 2), b.container.scrollLeft = Math.min(a.x, (b.container.scrollWidth - b.container.clientWidth) / 2))
    }), null, null, Editor.ctrlKey + "+Shift+J");
    this.addAction("fitPageWidth", mxUtils.bind(this, function() {
        b.pageVisible || this.get("pageView").funct();
        b.zoomTo(Math.floor(20 * (b.container.clientWidth - 10) / b.pageFormat.width / b.pageScale) / 20);
        if (mxUtils.hasScrollbars(b.container)) {
            var a = b.getPagePadding();
            b.container.scrollLeft = Math.min(a.x * b.view.scale, (b.container.scrollWidth - b.container.clientWidth) / 2)
        }
    }));
    this.put("customZoom", new Action(mxResources.get("custom") + "...", mxUtils.bind(this, function() {
        var a = new FilenameDialog(this.editorUi, parseInt(100 * b.getView().getScale()), mxResources.get("apply"), mxUtils.bind(this, function(a) {
            a = parseInt(a);
            !isNaN(a) && 0 < a && b.zoomTo(a / 100)
        }), mxResources.get("zoom") + " (%)");
        this.editorUi.showDialog(a.container, 300, 80, !0, !0);
        a.init()
    }), null, null, Editor.ctrlKey + "+0"));
    this.addAction("pageScale...", mxUtils.bind(this, function() {
        var a = new FilenameDialog(this.editorUi, parseInt(100 * b.pageScale), mxResources.get("apply"), mxUtils.bind(this, function(a) {
            a = parseInt(a);
            !isNaN(a) && 0 < a && (a = new ChangePageSetup(c, null, null, null, a / 100), a.ignoreColor = !0, a.ignoreImage = !0, b.model.execute(a))
        }), mxResources.get("pageScale") + " (%)");
        this.editorUi.showDialog(a.container, 300, 80, !0, !0);
        a.init()
    }));
    var e = null,
        e = this.addAction("grid", function() {
            b.setGridEnabled(!b.isGridEnabled());
            c.fireEvent(new mxEventObject("gridEnabledChanged"))
        }, null, null, Editor.ctrlKey + "+Shift+G");
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.isGridEnabled()
    });
    e.setEnabled(!1);
    e = this.addAction("guides", function() {
        b.graphHandler.guidesEnabled = !b.graphHandler.guidesEnabled;
        c.fireEvent(new mxEventObject("guidesEnabledChanged"))
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.graphHandler.guidesEnabled
    });
    e.setEnabled(!1);
    e = this.addAction("tooltips", function() {
        b.tooltipHandler.setEnabled(!b.tooltipHandler.isEnabled());
        c.fireEvent(new mxEventObject("tooltipsEnabledChanged"))
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.tooltipHandler.isEnabled()
    });
    e = this.addAction("collapseExpand", function() {
        var a = new ChangePageSetup(c);
        a.ignoreColor = !0;
        a.ignoreImage = !0;
        a.foldingEnabled = !b.foldingEnabled;
        b.model.execute(a)
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.foldingEnabled
    });
    e.isEnabled = f;
    e = this.addAction("scrollbars", function() {
        c.setScrollbars(!c.hasScrollbars())
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.scrollbars
    });
    e = this.addAction("pageView", mxUtils.bind(this, function() {
        c.setPageVisible(!b.pageVisible)
    }));
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.pageVisible
    });
    e = this.addAction("connectionArrows", function() {
        b.connectionArrowsEnabled = !b.connectionArrowsEnabled;
        c.fireEvent(new mxEventObject("connectionArrowsChanged"))
    }, null, null, "Alt+Shift+A");
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.connectionArrowsEnabled
    });
    e = this.addAction("connectionPoints", function() {
        b.setConnectable(!b.connectionHandler.isEnabled());
        c.fireEvent(new mxEventObject("connectionPointsChanged"))
    }, null, null, "Alt+Shift+P");
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.connectionHandler.isEnabled()
    });
    e = this.addAction("copyConnect", function() {
        b.connectionHandler.setCreateTarget(!b.connectionHandler.isCreateTarget());
        c.fireEvent(new mxEventObject("copyConnectChanged"))
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return b.connectionHandler.isCreateTarget()
    });
    e.isEnabled = f;
    e = this.addAction("autosave", function() {
        c.editor.setAutosave(!c.editor.autosave)
    });
    e.setToggleAction(!0);
    e.setSelectedCallback(function() {
        return c.editor.autosave
    });
    e.isEnabled = f;
    e.visible = !1;
    this.addAction("help", function() {
        var a = "";
        mxResources.isLanguageSupported(mxClient.language) && (a = "_" + mxClient.language);
        b.openLink(RESOURCES_PATH + "/help" + a + ".html")
    });
    var h = !1;
    this.put("about", new Action(mxResources.get("about") + " Graph Editor...", function() {
        h || (c.showDialog((new AboutDialog(c)).container, 320, 280, !0, !0, function() {
            h = !1
        }), h = !0)
    }));
    e = mxUtils.bind(this, function(a, c, d, e) {
        return this.addAction(a, function() {
            if (null != d && b.cellEditor.isContentEditing()) d();
            else {
                b.stopEditing(!1);
                b.getModel().beginUpdate();
                try {
                    var a = b.getSelectionCells();
                    b.toggleCellStyleFlags(mxConstants.STYLE_FONTSTYLE, c, a);
                    (c & mxConstants.FONT_BOLD) == mxConstants.FONT_BOLD ? b.updateLabelElements(b.getSelectionCells(), function(a) {
                        a.style.fontWeight = null;
                        "B" == a.nodeName && b.replaceElement(a)
                    }) : (c & mxConstants.FONT_ITALIC) == mxConstants.FONT_ITALIC ? b.updateLabelElements(b.getSelectionCells(), function(a) {
                        a.style.fontStyle = null;
                        "I" == a.nodeName && b.replaceElement(a)
                    }) : (c & mxConstants.FONT_UNDERLINE) == mxConstants.FONT_UNDERLINE && b.updateLabelElements(b.getSelectionCells(), function(a) {
                        a.style.textDecoration = null;
                        "U" == a.nodeName && b.replaceElement(a)
                    });
                    for (var e = 0; e < a.length; e++) 0 == b.model.getChildCount(a[e]) && b.autoSizeCell(a[e], !1)
                } finally {
                    b.getModel().endUpdate()
                }
            }
        }, null, null, e)
    });
    e("bold", mxConstants.FONT_BOLD, function() {
        document.execCommand("bold", !1, null)
    }, Editor.ctrlKey + "+B");
    e("italic", mxConstants.FONT_ITALIC, function() {
        document.execCommand("italic", !1, null)
    }, Editor.ctrlKey + "+I");
    e("underline", mxConstants.FONT_UNDERLINE, function() {
        document.execCommand("underline", !1, null)
    }, Editor.ctrlKey + "+U");
    this.addAction("fontColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_FONTCOLOR, "forecolor", "000000")
    });
    this.addAction("strokeColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_STROKECOLOR)
    });
    this.addAction("fillColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_FILLCOLOR)
    });
    this.addAction("gradientColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_GRADIENTCOLOR)
    });
    this.addAction("backgroundColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_LABEL_BACKGROUNDCOLOR, "backcolor")
    });
    this.addAction("borderColor...", function() {
        c.menus.pickColor(mxConstants.STYLE_LABEL_BORDERCOLOR)
    });
    this.addAction("vertical", function() {
        c.menus.toggleStyle(mxConstants.STYLE_HORIZONTAL, !0)
    });
    this.addAction("shadow", function() {
        c.menus.toggleStyle(mxConstants.STYLE_SHADOW)
    });
    this.addAction("solid", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_DASHED, null), b.setCellStyles(mxConstants.STYLE_DASH_PATTERN, null), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_DASHED, mxConstants.STYLE_DASH_PATTERN], "values", [null, null], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("dashed", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_DASHED, "1"), b.setCellStyles(mxConstants.STYLE_DASH_PATTERN, null), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_DASHED,
                mxConstants.STYLE_DASH_PATTERN
            ], "values", ["1", null], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("dotted", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_DASHED, "1"), b.setCellStyles(mxConstants.STYLE_DASH_PATTERN, "1 4"), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_DASHED, mxConstants.STYLE_DASH_PATTERN], "values", ["1", "1 4"], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("sharp", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_ROUNDED, "0"), b.setCellStyles(mxConstants.STYLE_CURVED, "0"), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_ROUNDED, mxConstants.STYLE_CURVED], "values", ["0", "0"], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("rounded", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_ROUNDED, "1"), b.setCellStyles(mxConstants.STYLE_CURVED, "0"), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_ROUNDED, mxConstants.STYLE_CURVED], "values", ["1", "0"], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("toggleRounded", function() {
        if (!b.isSelectionEmpty() && b.isEnabled()) {
            b.getModel().beginUpdate();
            try {
                var a = b.getSelectionCells(),
                    d = b.getCurrentCellStyle(a[0]),
                    e = "1" == mxUtils.getValue(d, mxConstants.STYLE_ROUNDED, "0") ? "0" : "1";
                b.setCellStyles(mxConstants.STYLE_ROUNDED, e);
                b.setCellStyles(mxConstants.STYLE_CURVED, null);
                c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_ROUNDED, mxConstants.STYLE_CURVED], "values", [e, "0"], "cells", b.getSelectionCells()))
            } finally {
                b.getModel().endUpdate()
            }
        }
    });
    this.addAction("curved", function() {
        b.getModel().beginUpdate();
        try {
            b.setCellStyles(mxConstants.STYLE_ROUNDED, "0"), b.setCellStyles(mxConstants.STYLE_CURVED, "1"), c.fireEvent(new mxEventObject("styleChanged", "keys", [mxConstants.STYLE_ROUNDED, mxConstants.STYLE_CURVED], "values", ["0", "1"], "cells", b.getSelectionCells()))
        } finally {
            b.getModel().endUpdate()
        }
    });
    this.addAction("collapsible", function() {
        var a = b.view.getState(b.getSelectionCell()),
            d = "1";
        null != a && null != b.getFoldingImage(a) && (d = "0");
        b.setCellStyles("collapsible", d);
        c.fireEvent(new mxEventObject("styleChanged", "keys", ["collapsible"], "values", [d], "cells", b.getSelectionCells()))
    });
    this.addAction("editStyle...", mxUtils.bind(this, function() {
        var a = b.getSelectionCells();
        if (null != a && 0 < a.length) {
            var c = b.getModel(),
                c = new TextareaDialog(this.editorUi, mxResources.get("editStyle") + ":", c.getStyle(a[0]) || "", function(c) {
                    null != c && b.setCellStyle(mxUtils.trim(c), a)
                }, null, null, 400, 220);
            this.editorUi.showDialog(c.container, 420, 300, !0, !0);
            c.init()
        }
    }), null, null, Editor.ctrlKey + "+E");

    this.addAction("setAsDefaultStyle", function() {
        b.isEnabled() && !b.isSelectionEmpty() && c.setDefaultStyle(b.getSelectionCell())
    }, null, null, Editor.ctrlKey + "+Shift+D");

    this.addAction("editseeAlso", mxUtils.bind(this, function() {
        var graph = b;
        var selectedCells = graph.getSelectionCells();
        // console.log(setStyleArray(selectedCells[0]));
        var cell = selectedCells[0];
        var cell_style = setStyleArray(selectedCells[0]);
        console.log(cell_style);
        if (cell_style != null) {
            if ("seeAlso" in cell_style) {
                var seeAlso_val = cell_style["seeAlso"];
            } else {
                seeAlso_val = "";
            }
        } else {
            seeAlso_val = "";
        }

        if (null != a && 0 < a.length) {
            var graphModel = graph.getModel();
            graphModel = new TextareaDialog(this.editorUi, "edit seeAlso:", seeAlso_val, function(graphModel) {
                var seeAlso_tmp = mxUtils.ltrim(graphModel);
                cell_style["seeAlso"] = seeAlso_tmp;
                cell.style = setStyleString(cell_style);

                if (seeAlso_tmp != "") {
                    //  selectedCells[0].seeAlso = seeAlso_tmp;
                    mxUtils.setTextContent(seeAlso_div, "seeAlso : \r\n" + seeAlso_tmp);
                } else {
                    mxUtils.setTextContent(seeAlso_div, "seeAlso : none");
                }

            }, null, null, 400, 220);

            this.editorUi.showDialog(graphModel.container, 420, 300, !0, !0);
            graphModel.init()
        }
    }), null, null, null);

    /***Set Container in the Same Layer***/
    this.addAction("setProblemSolvingContainer", function() {
        b.setSelectionCells(b.setProblemSolvingContainer());
    }, null, null, null);
    this.addAction("setInformationOperationContainer", function() {
        b.setSelectionCells(b.setInformationOperationContainer());
    }, null, null, null);
    this.addAction("setPhysicalOperationContainer", function() {
        b.setSelectionCells(b.setPhysicalOperationContainer());
    }, null, null, null);
    this.addAction("callPython", function() {
        b.setSelectionCells(b.callPython());
    }, null, null, null);

    this.addAction("clearDefaultStyle", function() {
        b.isEnabled() && c.clearDefaultStyle()
    }, null, null, Editor.ctrlKey + "+Shift+R");
    this.addAction("addWaypoint", function() {
        var a = b.getSelectionCell();
        if (null != a && b.getModel().isEdge(a)) {
            var c = d.graph.selectionCellsHandler.getHandler(a);
            if (c instanceof mxEdgeHandler) {
                for (var e = b.view.translate, f = b.view.scale, h = e.x, e = e.y, a = b.getModel().getParent(a), p = b.getCellGeometry(a); b.getModel().isVertex(a) && null != p;) h += p.x, e += p.y, a = b.getModel().getParent(a), p = b.getCellGeometry(a);
                h = Math.round(b.snap(b.popupMenuHandler.triggerX / f - h));
                f = Math.round(b.snap(b.popupMenuHandler.triggerY / f - e));
                c.addPointAt(c.state, h, f)
            }
        }
    });
    this.addAction("removeWaypoint", function() {
        var a = c.actions.get("removeWaypoint");
        null != a.handler && a.handler.removePoint(a.handler.state, a.index)
    });
    this.addAction("clearWaypoints", function() {
        var a = b.getSelectionCells();
        if (null != a) {
            a = b.addAllEdges(a);
            b.getModel().beginUpdate();
            try {
                for (var c = 0; c < a.length; c++) {
                    var d = a[c];
                    if (b.getModel().isEdge(d)) {
                        var e = b.getCellGeometry(d);
                        null != e && (e = e.clone(), e.points = null, b.getModel().setGeometry(d, e))
                    }
                }
            } finally {
                b.getModel().endUpdate()
            }
        }
    }, null, null, "Alt+Shift+C");
    e = this.addAction("subscript", mxUtils.bind(this, function() {
        b.cellEditor.isContentEditing() && document.execCommand("subscript", !1, null)
    }), null, null, Editor.ctrlKey + "+,");
    e = this.addAction("superscript", mxUtils.bind(this, function() {
        b.cellEditor.isContentEditing() && document.execCommand("superscript", !1, null)
    }), null, null, Editor.ctrlKey + "+.");
    e = this.addAction("indent", mxUtils.bind(this, function() {
        b.cellEditor.isContentEditing() && document.execCommand("indent", !1, null)
    }), null, null, "Shift+Tab");
    this.addAction("image...", function() {
        if (b.isEnabled() && !b.isCellLocked(b.getDefaultParent())) {
            var a = mxResources.get("image") + " (" + mxResources.get("url") + "):",
                d = b.getView().getState(b.getSelectionCell()),
                e = "";
            null != d && (e = d.style[mxConstants.STYLE_IMAGE] || e);
            var f = b.cellEditor.saveSelection();
            c.showImageDialog(a, e, function(a, c, d) {
                if (b.cellEditor.isContentEditing()) b.cellEditor.restoreSelection(f), b.insertImage(a, c, d);
                else {
                    var e = b.getSelectionCells();
                    if (null != a && (0 < a.length || 0 < e.length)) {
                        var g = null;
                        b.getModel().beginUpdate();
                        try {
                            if (0 == e.length) {
                                var e = [b.insertVertex(b.getDefaultParent(), null, "", 0, 0, c, d, "shape=image;imageAspect=0;aspect=fixed;verticalLabelPosition=bottom;verticalAlign=top;")],
                                    h = b.getCenterInsertPoint(b.getBoundingBoxFromGeometry(e, !0));
                                e[0].geometry.x = h.x;
                                e[0].geometry.y = h.y;
                                g = e;
                                b.fireEvent(new mxEventObject("cellsInserted", "cells", g))
                            }
                            b.setCellStyles(mxConstants.STYLE_IMAGE, 0 < a.length ? a : null, e);
                            var k = b.getCurrentCellStyle(e[0]);
                            "image" != k[mxConstants.STYLE_SHAPE] && "label" != k[mxConstants.STYLE_SHAPE] ? b.setCellStyles(mxConstants.STYLE_SHAPE, "image", e) : 0 == a.length && b.setCellStyles(mxConstants.STYLE_SHAPE, null, e);
                            if (1 == b.getSelectionCount() && null != c && null != d) {
                                var l = e[0],
                                    m = b.getModel().getGeometry(l);
                                null != m && (m = m.clone(), m.width = c, m.height = d, b.getModel().setGeometry(l, m))
                            }
                        } finally {
                            b.getModel().endUpdate()
                        }
                        null != g && (b.setSelectionCells(g), b.scrollCellToVisible(g[0]))
                    }
                }
            }, b.cellEditor.isContentEditing(), !b.cellEditor.isContentEditing())
        }
    }).isEnabled = f;
    e = this.addAction("layers", mxUtils.bind(this, function() {
        null == this.layersWindow ? (this.layersWindow = new LayersWindow(c, document.body.offsetWidth - 280, 120, 220, 196), this.layersWindow.window.addListener("show", function() {
            c.fireEvent(new mxEventObject("layers"))
        }), this.layersWindow.window.addListener("hide", function() {
            c.fireEvent(new mxEventObject("layers"))
        }), this.layersWindow.window.setVisible(!0), c.fireEvent(new mxEventObject("layers")), this.layersWindow.init()) : this.layersWindow.window.setVisible(!this.layersWindow.window.isVisible())
    }), null, null, Editor.ctrlKey + "+Shift+L");
    e.setToggleAction(!0);
    e.setSelectedCallback(mxUtils.bind(this, function() {
        return null != this.layersWindow && this.layersWindow.window.isVisible()
    }));
    e = this.addAction("formatPanel", mxUtils.bind(this, function() {
        c.toggleFormatPanel()
    }), null, null, Editor.ctrlKey + "+Shift+P");
    e.setToggleAction(!0);
    e.setSelectedCallback(mxUtils.bind(this, function() {
        return 0 < c.formatWidth
    }));
    e = this.addAction("outline", mxUtils.bind(this, function() {
        null == this.outlineWindow ? (this.outlineWindow = new OutlineWindow(c, document.body.offsetWidth - 260, 100, 180, 180), this.outlineWindow.window.addListener("show", function() {
            c.fireEvent(new mxEventObject("outline"))
        }), this.outlineWindow.window.addListener("hide", function() {
            c.fireEvent(new mxEventObject("outline"))
        }), this.outlineWindow.window.setVisible(!0), c.fireEvent(new mxEventObject("outline"))) : this.outlineWindow.window.setVisible(!this.outlineWindow.window.isVisible())
    }), null, null, Editor.ctrlKey + "+Shift+O");
    e.setToggleAction(!0);
    e.setSelectedCallback(mxUtils.bind(this, function() {
        return null != this.outlineWindow && this.outlineWindow.window.isVisible()
    }))
};


Menus.prototype.addPopupMenuStyleItems = function(a, c, d) {
    1 == this.editorUi.editor.graph.getSelectionCount() ? this.addMenuItems(a, ["-", "setAsDefaultStyle", "-", "setProblemSolvingContainer", "setInformationOperationContainer", "setPhysicalOperationContainer", "-", "callPython"], null, d) : this.editorUi.editor.graph.isSelectionEmpty() && this.addMenuItems(a, ["-", "clearDefaultStyle"], null, d)
};

(function() {
    EditorUi.prototype.altShiftActions[68] = "selectDescendants";
    var a = Graph.prototype.foldCells;
    Graph.prototype.foldCells = function(b, c, e, l, d) {
        c = null != c ? c : !1;
        null == e && (e = this.getFoldableCells(this.getSelectionCells(), b));
        this.stopEditing();
        this.model.beginUpdate();
        try {
            for (var f = e.slice(), k = [], n = 0; n < e.length; n++) {
                var p = this.getCurrentCellStyle(e[n]);
                "1" == mxUtils.getValue(p, "treeFolding", "0") && (this.traverse(e[n], !0, mxUtils.bind(this, function(a, b) {
                    null != b && k.push(b);
                    a != e[n] && k.push(a);
                    return a == e[n] || !this.model.isCollapsed(a)
                })), this.model.setCollapsed(e[n], b))
            }
            for (n = 0; n < k.length; n++) this.model.setVisible(k[n], !b);
            e = f;
            e = a.apply(this, arguments)
        } finally {
            this.model.endUpdate()
        }
        return e
    };
    var e = EditorUi.prototype.init;
    EditorUi.prototype.init = function() {
        e.apply(this, arguments);
        this.editor.isChromelessView() && !this.editor.editable || this.addTrees()
    };
    EditorUi.prototype.addTrees = function() {
        function a(a) {
            return v.isVertex(a) && e(a)
        }

        function c(a) {
            var b = !1;
            null != a && (b = "1" == q.getCurrentCellStyle(a).treeMoving);
            return b
        }

        function e(a) {
            var b = !1;
            null != a && (a = v.getParent(a), b = q.view.getState(a), b = "tree" == (null != b ? b.style : q.getCellStyle(a)).containerType);
            return b
        }

        function l(a) {
            var b = !1;
            null != a && (a = v.getParent(a), b = q.view.getState(a), q.view.getState(a), b = null != (null != b ? b.style : q.getCellStyle(a)).childLayout);
            return b
        }

        function d(a) {
            a = q.view.getState(a);
            if (null != a) {
                var b = q.getIncomingEdges(a.cell);
                if (0 < b.length && (b = q.view.getState(b[0]), null != b && (b = b.absolutePoints, null != b && 0 < b.length && (b = b[b.length - 1], null != b)))) {
                    if (b.y == a.y && Math.abs(b.x - a.getCenterX()) < a.width / 2) return mxConstants.DIRECTION_SOUTH;
                    if (b.y == a.y + a.height && Math.abs(b.x - a.getCenterX()) < a.width / 2) return mxConstants.DIRECTION_NORTH;
                    if (b.x > a.getCenterX()) return mxConstants.DIRECTION_WEST
                }
            }
            return mxConstants.DIRECTION_EAST
        }

        function g(a, b) {
            b = null != b ? b : !0;
            q.model.beginUpdate();
            try {
                var c = q.model.getParent(a),
                    e = q.getIncomingEdges(a),
                    f = q.cloneCells([e[0], a]);
                q.model.setTerminal(f[0], q.model.getTerminal(e[0], !0), !0);
                var g = d(a),
                    k = c.geometry;
                g == mxConstants.DIRECTION_SOUTH || g == mxConstants.DIRECTION_NORTH ? f[1].geometry.x += b ? a.geometry.width + 10 : -f[1].geometry.width - 10 : f[1].geometry.y += b ? a.geometry.height + 10 : -f[1].geometry.height - 10;
                q.view.currentRoot != c && (f[1].geometry.x -= k.x, f[1].geometry.y -= k.y);
                var l = q.view.getState(a),
                    m = q.view.scale;
                if (null != l) {
                    var n = mxRectangle.fromRectangle(l);
                    g == mxConstants.DIRECTION_SOUTH || g == mxConstants.DIRECTION_NORTH ? n.x += (b ? a.geometry.width + 10 : -f[1].geometry.width - 10) * m : n.y += (b ? a.geometry.height + 10 : -f[1].geometry.height - 10) * m;
                    var p = q.getOutgoingEdges(q.model.getTerminal(e[0], !0));
                    if (null != p) {
                        for (var v = g == mxConstants.DIRECTION_SOUTH || g == mxConstants.DIRECTION_NORTH, u = k = e = 0; u < p.length; u++) {
                            var t = q.model.getTerminal(p[u], !1);
                            if (g == d(t)) {
                                var z = q.view.getState(t);
                                t != a && null != z && (v && b != z.getCenterX() < l.getCenterX() || !v && b != z.getCenterY() < l.getCenterY()) && mxUtils.intersects(n, z) && (e = 10 + Math.max(e, (Math.min(n.x + n.width, z.x + z.width) - Math.max(n.x, z.x)) / m), k = 10 + Math.max(k, (Math.min(n.y + n.height, z.y + z.height) - Math.max(n.y, z.y)) / m))
                            }
                        }
                        v ? k = 0 : e = 0;
                        for (u = 0; u < p.length; u++)
                            if (t = q.model.getTerminal(p[u], !1), g == d(t) && (z = q.view.getState(t), t != a && null != z && (v && b != z.getCenterX() < l.getCenterX() || !v && b != z.getCenterY() < l.getCenterY()))) {
                                var x = [];
                                q.traverse(z.cell, !0, function(a, b) {
                                    null != b && x.push(b);
                                    x.push(a);
                                    return !0
                                });
                                q.moveCells(x, (b ? 1 : -1) * e, (b ? 1 : -1) * k)
                            }
                    }
                }
                return q.addCells(f, c)
            } finally {
                q.model.endUpdate()
            }
        }

        function m(a) {
            q.model.beginUpdate();
            try {
                var b = d(a),
                    c = q.getIncomingEdges(a),
                    e = q.cloneCells([c[0], a]);
                q.model.setTerminal(c[0], e[1], !1);
                q.model.setTerminal(e[0], e[1], !0);
                q.model.setTerminal(e[0], a, !1);
                var f = q.model.getParent(a),
                    g = f.geometry,
                    k = [];
                q.view.currentRoot != f && (e[1].geometry.x -= g.x, e[1].geometry.y -= g.y);
                q.traverse(a, !0, function(a, b) {
                    null != b && k.push(b);
                    k.push(a);
                    return !0
                });
                var l = a.geometry.width + 40,
                    m = a.geometry.height + 40;
                b == mxConstants.DIRECTION_SOUTH ? l = 0 : b == mxConstants.DIRECTION_NORTH ? (l = 0, m = -m) : b == mxConstants.DIRECTION_WEST ? (l = -l, m = 0) : b == mxConstants.DIRECTION_EAST && (m = 0);
                q.moveCells(k, l, m);
                return q.addCells(e, f)
            } finally {
                q.model.endUpdate()
            }
        }

        function n(a, b) {
            q.model.beginUpdate();
            try {
                var c = q.model.getParent(a),
                    e = q.getIncomingEdges(a),
                    f = d(a);
                0 == e.length && (e = [q.createEdge(c, null, "", null, null, q.createCurrentEdgeStyle())], f = b);
                var g = q.cloneCells([e[0], a]);
                q.model.setTerminal(g[0], a, !0);
                if (null == q.model.getTerminal(g[0], !1)) {
                    q.model.setTerminal(g[0], g[1], !1);
                    var k = q.getCellStyle(g[1]).newEdgeStyle;
                    if (null != k) try {
                        var l = JSON.parse(k),
                            m;
                        for (m in l) q.setCellStyles(m, l[m], [g[0]]), "edgeStyle" == m && "elbowEdgeStyle" == l[m] && q.setCellStyles("elbow", f == mxConstants.DIRECTION_SOUTH || f == mxConstants.DIRECTION_NOTH ? "vertical" : "horizontal", [g[0]])
                    } catch (P) {}
                }
                var e = q.getOutgoingEdges(a),
                    n = c.geometry,
                    k = [];
                q.view.currentRoot == c && (n = new mxRectangle);
                for (l = 0; l < e.length; l++) {
                    var p = q.model.getTerminal(e[l], !1);
                    null != p && k.push(p)
                }
                var v = q.view.getBounds(k),
                    u = q.view.translate,
                    t = q.view.scale;
                f == mxConstants.DIRECTION_SOUTH ? (g[1].geometry.x = null == v ? a.geometry.x + (a.geometry.width - g[1].geometry.width) / 2 : (v.x + v.width) / t - u.x - n.x + 10, g[1].geometry.y += g[1].geometry.height - n.y + 40) : f == mxConstants.DIRECTION_NORTH ? (g[1].geometry.x = null == v ? a.geometry.x + (a.geometry.width - g[1].geometry.width) / 2 : (v.x + v.width) / t - u.x + -n.x + 10, g[1].geometry.y -= g[1].geometry.height + n.y + 40) : (g[1].geometry.x = f == mxConstants.DIRECTION_WEST ? g[1].geometry.x - (g[1].geometry.width + n.x + 40) : g[1].geometry.x + (g[1].geometry.width - n.x + 40), g[1].geometry.y = null == v ? a.geometry.y + (a.geometry.height - g[1].geometry.height) / 2 : (v.y + v.height) / t - u.y + -n.y + 10);
                return q.addCells(g, c)
            } finally {
                q.model.endUpdate()
            }
        }

        function p(a, b, c) {
            a = q.getOutgoingEdges(a);
            c = q.view.getState(c);
            var d = [];
            if (null != c && null != a) {
                for (var e = 0; e < a.length; e++) {
                    var f = q.view.getState(q.model.getTerminal(a[e], !1));
                    null != f && (!b && Math.min(f.x + f.width, c.x + c.width) >= Math.max(f.x, c.x) || b && Math.min(f.y + f.height, c.y + c.height) >= Math.max(f.y, c.y)) && d.push(f)
                }
                d.sort(function(a, c) {
                    return b ? a.x + a.width - c.x - c.width : a.y + a.height - c.y - c.height
                })
            }
            return d
        }

        function t(a, b) {
            var c = d(a),
                e = b == mxConstants.DIRECTION_EAST || b == mxConstants.DIRECTION_WEST;
            (c == mxConstants.DIRECTION_EAST || c == mxConstants.DIRECTION_WEST) == e && c != b ? u.actions.get("selectParent").funct() : c == b ? (e = q.getOutgoingEdges(a), null != e && 0 < e.length && q.setSelectionCell(q.model.getTerminal(e[0], !1))) : (c = q.getIncomingEdges(a), null != c && 0 < c.length && (e = p(q.model.getTerminal(c[0], !0), e, a), c = q.view.getState(a), null != c && (c = mxUtils.indexOf(e, c), 0 <= c && (c += b == mxConstants.DIRECTION_NORTH || b == mxConstants.DIRECTION_WEST ? -1 : 1, 0 <= c && c <= e.length - 1 && q.setSelectionCell(e[c].cell)))))
        }
        var u = this,
            q = u.editor.graph,
            v = q.getModel(),
            x = u.menus.createPopupMenu;
        u.menus.createPopupMenu = function(b, c, d) {
            x.apply(this, arguments);
            if (1 == q.getSelectionCount()) {
                c = q.getSelectionCell();
                var e = q.getOutgoingEdges(c);
                b.addSeparator();
                0 < e.length && (a(q.getSelectionCell()) && this.addMenuItems(b, ["selectChildren"], null, d), this.addMenuItems(b, ["selectDescendants"], null, d));
                a(q.getSelectionCell()) && (b.addSeparator(), 0 < q.getIncomingEdges(c).length && this.addMenuItems(b, ["selectSiblings", "selectParent"], null, d))
            }
        };
        u.actions.addAction("selectChildren", function() {
            if (q.isEnabled() && 1 == q.getSelectionCount()) {
                var a = q.getSelectionCell(),
                    a = q.getOutgoingEdges(a);
                if (null != a) {
                    for (var b = [], c = 0; c < a.length; c++) b.push(q.model.getTerminal(a[c], !1));
                    q.setSelectionCells(b)
                }
            }
        }, null, null, "Alt+Shift+X");
        u.actions.addAction("selectSiblings", function() {
            if (q.isEnabled() && 1 == q.getSelectionCount()) {
                var a = q.getSelectionCell(),
                    a = q.getIncomingEdges(a);
                if (null != a && 0 < a.length && (a = q.getOutgoingEdges(q.model.getTerminal(a[0], !0)), null != a)) {
                    for (var b = [], c = 0; c < a.length; c++) b.push(q.model.getTerminal(a[c], !1));
                    q.setSelectionCells(b)
                }
            }
        }, null, null, "Alt+Shift+S");
        u.actions.addAction("selectParent", function() {
            if (q.isEnabled() && 1 == q.getSelectionCount()) {
                var a = q.getSelectionCell(),
                    a = q.getIncomingEdges(a);
                null != a && 0 < a.length && q.setSelectionCell(q.model.getTerminal(a[0], !0))
            }
        }, null, null, "Alt+Shift+P");
        u.actions.addAction("selectDescendants", function() {
            if (q.isEnabled() && 1 == q.getSelectionCount()) {
                var a = q.getSelectionCell(),
                    b = [];
                q.traverse(a, !0, function(a, c) {
                    null != c && b.push(c);
                    b.push(a);
                    return !0
                });
                q.setSelectionCells(b)
            }
        }, null, null, "Alt+Shift+D");
        var y = q.removeCells;
        q.removeCells = function(b, c) {
            c = null != c ? c : !0;
            null == b && (b = this.getDeletableCells(this.getSelectionCells()));
            c && (b = this.getDeletableCells(this.addAllEdges(b)));
            for (var d = [], f = 0; f < b.length; f++) {
                var g = b[f];
                v.isEdge(g) && e(g) && (d.push(g), g = v.getTerminal(g, !1));
                if (a(g)) {
                    var k = [];
                    q.traverse(g, !0, function(a, b) {
                        null != b && k.push(b);
                        k.push(a);
                        return !0
                    });
                    0 < k.length && (d = d.concat(k), g = q.getIncomingEdges(b[f]), b = b.concat(g))
                } else null != g && d.push(b[f])
            }
            b = d;
            return y.apply(this, arguments)
        };
        u.hoverIcons.getStateAt = function(b, c, d) {
            return a(b.cell) ? null : this.graph.view.getState(this.graph.getCellAt(c, d))
        };
        var A = q.duplicateCells;
        q.duplicateCells = function(b, c) {
            b = null != b ? b : this.getSelectionCells();
            for (var d = b.slice(0), e = 0; e < d.length; e++) {
                var f = q.view.getState(d[e]);
                if (null != f && a(f.cell))
                    for (var g = q.getIncomingEdges(f.cell), f = 0; f < g.length; f++) mxUtils.remove(g[f], b)
            }
            this.model.beginUpdate();
            try {
                var k = A.call(this, b, c);
                if (k.length == b.length)
                    for (e = 0; e < b.length; e++)
                        if (a(b[e])) {
                            var l = q.getIncomingEdges(k[e]),
                                g = q.getIncomingEdges(b[e]);
                            if (0 == l.length && 0 < g.length) {
                                var m = this.cloneCell(g[0]);
                                this.addEdge(m, q.getDefaultParent(), this.model.getTerminal(g[0], !0), k[e])
                            }
                        }
            } finally {
                this.model.endUpdate()
            }
            return k
        };

        //詳細化のために，親アクションのコンテナを設置する
        q.setProblemSolvingContainer = function(b, c) {

            //selectedCell:cell you select
            var selectedCell = this.getSelectionCell();
            var selectedCell_x = selectedCell.geometry.x;
            var selectedCell_y = selectedCell.geometry.y;

            //cell:container
            var cell = new mxCell(selectedCell.value, new mxGeometry(0, 0, 400, container_height), 'swimlane;pd3type=container;containertype=specialization');
            cell.vertex = true;

            //edge:arrow to connect selacted cell with container
            var edge = new mxCell('', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=1;exitX=0.5;exitY=0;pd3type=arrow;');

            //コンテナの座標を決める
            //選択したセルがコンテナに含まれているか否か
            //含まれている場合
            if (selectedCell.parent.style != null) {
                if (selectedCell.parent.style.indexOf('pd3type=container;') !== -1) {
                    //親アクションが存在する場合，子アクションの座標は親アクションに対する相対座標で表される．
                    //コンテナのx座標は，親コンテナのx座標 + 選択セルの相対x座標
                    cell.geometry.x = selectedCell.parent.geometry.x + selectedCell.geometry.x;
                    //コンテナのy座標は，親コンテナ左上角のy座標 + 親コンテナの高さ + 60
                    cell.geometry.y = selectedCell.parent.geometry.y + selectedCell.parent.geometry.height + 60;
                }
            } else { //含まれていない場合
                //コンテナのx座標は，選択したセルのx座標
                cell.geometry.x = selectedCell_x;
                //コンテナのy座標は，選択したセルの左上角のy座標 + 選択したセルの高さ + 60
                cell.geometry.y = selectedCell_y + selectedCell.geometry.height + 60;
            }

            //コンテナの左上角を矢印の根本に接続する．
            edge.geometry.setTerminalPoint(new mxPoint(cell.geometry.x + cell.geometry.width / 2, cell.geometry.y), true);
            //選択したセルの左下角をコンテナからの矢印の先端と接続する
            edge.geometry.setTerminalPoint(new mxPoint(selectedCell_x, selectedCell_y + selectedCell.geometry.height), false);
            edge.edge = true;

            //コンテナに矢印の根本を挿入する．
            cell.insertEdge(edge, true);
            //選択したセルに矢印の先端を接続する．
            selectedCell.insertEdge(edge, false);


            var pd3layer = "pd3layer=topic;",
                fillColor = "fillColor=#ffe6cc;",
                strokeColor = "strokeColor=#d79b00;";

            cell.style = cell.style + pd3layer + fillColor + strokeColor;
            edge.style = edge.style + pd3layer + fillColor + strokeColor;


            this.addCell(cell);
            this.addCell(edge);

            return cell, edge;
        };
        q.setInformationOperationContainer = function(b, c) {

            //selectedCell:cell you select
            var selectedCell = this.getSelectionCell();
            var selectedCell_x = selectedCell.geometry.x;
            var selectedCell_y = selectedCell.geometry.y;

            //cell:container
            var cell = new mxCell(selectedCell.value, new mxGeometry(0, 0, 400, container_height), 'swimlane;pd3type=container;containertype=specialization');
            cell.vertex = true;

            //edge:arrow to connect selacted cell with container
            var edge = new mxCell('', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=1;exitX=0.5;exitY=0;pd3type=arrow;');

            //コンテナの座標を決める
            //選択したセルがコンテナに含まれているか否か
            //含まれている場合
            if (selectedCell.parent.style != null) {
                if (selectedCell.parent.style.indexOf('pd3type=container;') !== -1) {
                    //親アクションが存在する場合，子アクションの座標は親アクションに対する相対座標で表される．
                    //コンテナのx座標は，親コンテナのx座標 + 選択セルの相対x座標
                    cell.geometry.x = selectedCell.parent.geometry.x + selectedCell.geometry.x;
                    //コンテナのy座標は，親コンテナ左上角のy座標 + 親コンテナの高さ + 60
                    cell.geometry.y = selectedCell.parent.geometry.y + selectedCell.parent.geometry.height + 60;
                }
            } else { //含まれていない場合
                //コンテナのx座標は，選択したセルのx座標
                cell.geometry.x = selectedCell_x;
                //コンテナのy座標は，選択したセルの左上角のy座標 + 選択したセルの高さ + 60
                cell.geometry.y = selectedCell_y + selectedCell.geometry.height + 60;
            }

            //コンテナの左上角を矢印の根本に接続する．
            edge.geometry.setTerminalPoint(new mxPoint(cell.geometry.x + cell.geometry.width / 2, cell.geometry.y), true);
            //選択したセルの左下角をコンテナからの矢印の先端と接続する
            edge.geometry.setTerminalPoint(new mxPoint(selectedCell_x, selectedCell_y + selectedCell.geometry.height), false);
            edge.edge = true;

            //コンテナに矢印の根本を挿入する．
            cell.insertEdge(edge, true);
            //選択したセルに矢印の先端を接続する．
            selectedCell.insertEdge(edge, false);



            var pd3layer = "pd3layer=info;",
                fillColor = "fillColor=#dae8fc;",
                strokeColor = "strokeColor=#6c8ebf;";

            cell.style = cell.style + pd3layer + fillColor + strokeColor;
            edge.style = edge.style + pd3layer + fillColor + strokeColor;


            this.addCell(cell);
            this.addCell(edge);

            return cell, edge;
        };
        q.setPhysicalOperationContainer = function(b, c) {

            //selectedCell:cell you select
            var selectedCell = this.getSelectionCell();
            var selectedCell_x = selectedCell.geometry.x;
            var selectedCell_y = selectedCell.geometry.y;

            //cell:container
            var cell = new mxCell(selectedCell.value, new mxGeometry(0, 0, 400, container_height), 'swimlane;pd3type=container;containertype=specialization');
            cell.vertex = true;

            //edge:arrow to connect selacted cell with container
            var edge = new mxCell('', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=1;exitX=0.5;exitY=0;pd3type=arrow;');

            //コンテナの座標を決める
            //選択したセルがコンテナに含まれているか否か
            //含まれている場合
            if (selectedCell.parent.style != null) {
                if (selectedCell.parent.style.indexOf('pd3type=container;') !== -1) {
                    //親アクションが存在する場合，子アクションの座標は親アクションに対する相対座標で表される．
                    //コンテナのx座標は，親コンテナのx座標 + 選択セルの相対x座標
                    cell.geometry.x = selectedCell.parent.geometry.x + selectedCell.geometry.x;
                    //コンテナのy座標は，親コンテナ左上角のy座標 + 親コンテナの高さ + 60
                    cell.geometry.y = selectedCell.parent.geometry.y + selectedCell.parent.geometry.height + 60;
                }
            } else { //含まれていない場合
                //コンテナのx座標は，選択したセルのx座標
                cell.geometry.x = selectedCell_x;
                //コンテナのy座標は，選択したセルの左上角のy座標 + 選択したセルの高さ + 60
                cell.geometry.y = selectedCell_y + selectedCell.geometry.height + 60;
            }

            //コンテナの左上角を矢印の根本に接続する．
            edge.geometry.setTerminalPoint(new mxPoint(cell.geometry.x + cell.geometry.width / 2, cell.geometry.y), true);
            //選択したセルの左下角をコンテナからの矢印の先端と接続する
            edge.geometry.setTerminalPoint(new mxPoint(selectedCell_x, selectedCell_y + selectedCell.geometry.height), false);
            edge.edge = true;

            //コンテナに矢印の根本を挿入する．
            cell.insertEdge(edge, true);
            //選択したセルに矢印の先端を接続する．
            selectedCell.insertEdge(edge, false);

            var pd3layer = "pd3layer=phys;",
                fillColor = "fillColor=#d5e8d4;",
                strokeColor = "strokeColor=#82b366;";

            cell.style = cell.style + pd3layer + fillColor + strokeColor;
            edge.style = edge.style + pd3layer + fillColor + strokeColor;


            this.addCell(cell);
            this.addCell(edge);

            return cell, edge;
        };

        //to call python code
        q.callPython = function(b, c) {
            console.log("calling python");
        };

        //親アクションのエンジニアリングサイクルコンテナを設置する
        q.setContainerforEC = function(b, c) {
            //selectedCell:cell you select
            var selectedCell = this.getSelectionCell();
            var selectedCell_x = selectedCell.geometry.x;
            var selectedCell_y = selectedCell.geometry.y;

            //cell:container
            var cell = new mxCell(selectedCell.value, new mxGeometry(0, 0, containerforEC_width, container_height), 'swimlane;rounded=1;pd3type=container;containertype=ec');
            cell.vertex = true;

            //edge:arrow to connect selected cell with container
            //entryX=0;entryY=1; : アクションの幅，高さをそれぞれ1とした時に，入力矢印（矢印の先端）をアクションの左下に接続する
            //exitX=0.5;exitY=0; : コンテナの幅，高さをそれぞれ1としたときに，出力矢印（矢印の根本）をコンテナの上辺の中点に接続する
            var edge = new mxCell('', new mxGeometry(0, 0, 0, 0), 'html=1;verticalAlign=bottom;endArrow=block;rounded=0;entryX=0;entryY=1;exitX=0.5;exitY=0;pd3type=arrow;');

            //コンテナの座標を決める
            //選択したセルがコンテナに含まれているか否か
            //含まれている場合
            if (selectedCell.parent.style != null) {
                if (selectedCell.parent.style.indexOf('pd3type=container;') !== -1) {
                    //親アクションが存在する場合，子アクションの座標は親アクションに対する相対座標で表される．
                    //コンテナのx座標は，親コンテナのx座標 + 選択セルの相対x座標 + コンテナの幅の半分(100)
                    cell.geometry.x = selectedCell.parent.geometry.x　 + 　selectedCell.geometry.x;
                    //コンテナのy座標は，親コンテナ左上角のy座標 + 親コンテナの高さ + 60
                    cell.geometry.y = selectedCell.parent.geometry.y + selectedCell.parent.geometry.height + 60;
                }
            } else { //含まれていない場合
                //コンテナのx座標は，選択したセルのx座標+コンテナの幅の半分(100)
                cell.geometry.x = selectedCell_x;
                //コンテナのy座標は，選択したセルの左上角のy座標 + 選択したセルの高さ + 60
                cell.geometry.y = selectedCell_y + selectedCell.geometry.height + 60;
            }

            //コンテナの中心を矢印の根本に接続する．
            edge.geometry.setTerminalPoint(new mxPoint(cell.geometry.x + cell.geometry.width / 2, cell.geometry.y), true);
            //選択したセルの左下角をコンテナからの矢印の先端と接続する
            edge.geometry.setTerminalPoint(new mxPoint(selectedCell_x, selectedCell_y + selectedCell.geometry.height), false);
            edge.edge = true;

            //コンテナに矢印の根本を挿入する．
            cell.insertEdge(edge, true);
            //選択したセルに矢印の先端を接続する．
            selectedCell.insertEdge(edge, false);

            var pd3layer = "pd3layer=topic;",
                fillColor = "fillColor=#ffe6cc;",
                strokeColor = "strokeColor=#d79b00;";

            cell.style = cell.style + pd3layer + fillColor + strokeColor;
            edge.style = edge.style + pd3layer + fillColor + strokeColor;

            this.addCell(cell);
            this.addCell(edge);

            return cell, edge;
        };

        var z = q.moveCells;
        q.moveCells = function(b, c, d, e, f, g, k) {
            var l = null;
            this.model.beginUpdate();
            try {
                var m = f,
                    n = this.getCurrentCellStyle(f);
                if (null != b && a(f) && "1" == mxUtils.getValue(n, "treeFolding", "0")) {
                    for (var p = 0; p < b.length; p++)
                        if (a(b[p]) || q.model.isEdge(b[p]) && null == q.model.getTerminal(b[p], !0)) {
                            f = q.model.getParent(b[p]);
                            break
                        }
                    if (null != m && f != m && null != this.view.getState(b[0])) {
                        var v = q.getIncomingEdges(b[0]);
                        if (0 < v.length) {
                            var u = q.view.getState(q.model.getTerminal(v[0], !0));
                            if (null != u) {
                                var t = q.view.getState(m);
                                null != t && (c = (t.getCenterX() - u.getCenterX()) / q.view.scale, d = (t.getCenterY() - u.getCenterY()) / q.view.scale)
                            }
                        }
                    }
                }
                l = z.apply(this, arguments);
                if (null != l && null != b && l.length == b.length)
                    for (p = 0; p < l.length; p++)
                        if (this.model.isEdge(l[p])) a(m) && 0 > mxUtils.indexOf(l, this.model.getTerminal(l[p], !0)) && this.model.setTerminal(l[p], m, !0);
                        else if (a(b[p]) && (v = q.getIncomingEdges(b[p]), 0 < v.length))
                    if (!e) a(m) && 0 > mxUtils.indexOf(b, this.model.getTerminal(v[0], !0)) && this.model.setTerminal(v[0], m, !0);
                    else if (0 == q.getIncomingEdges(l[p]).length) {
                    n = m;
                    if (null == n || n == q.model.getParent(b[p])) n = q.model.getTerminal(v[0], !0);
                    e = this.cloneCell(v[0]);
                    this.addEdge(e, q.getDefaultParent(), n, l[p])
                }
            } finally {
                this.model.endUpdate()
            }
            return l
        };
        if (null != u.sidebar) {
            var B = u.sidebar.dropAndConnect;
            u.sidebar.dropAndConnect = function(b, c, d, e) {
                var f = q.model,
                    g = null;
                f.beginUpdate();
                try {
                    if (g = B.apply(this, arguments), a(b))
                        for (var k = 0; k < g.length; k++)
                            if (f.isEdge(g[k]) && null == f.getTerminal(g[k], !0)) {
                                f.setTerminal(g[k], b, !0);
                                var l = q.getCellGeometry(g[k]);
                                l.points = null;
                                null != l.getTerminalPoint(!0) && l.setTerminalPoint(null, !0)
                            }
                } finally {
                    f.endUpdate()
                }
                return g
            }
        }
        var C = {
                88: u.actions.get("selectChildren"),
                84: u.actions.get("selectSubtree"),
                80: u.actions.get("selectParent"),
                83: u.actions.get("selectSiblings")
            },
            D = u.onKeyDown;
        u.onKeyDown = function(b) {
            try {
                if (q.isEnabled() && !q.isEditing() && a(q.getSelectionCell()) && 1 == q.getSelectionCount()) {
                    var c = null;
                    0 < q.getIncomingEdges(q.getSelectionCell()).length && (9 == b.which ? c = mxEvent.isShiftDown(b) ? m(q.getSelectionCell()) : n(q.getSelectionCell()) : 13 == b.which && (c = g(q.getSelectionCell(), !mxEvent.isShiftDown(b))));
                    if (null != c && 0 < c.length) 1 == c.length && q.model.isEdge(c[0]) ? q.setSelectionCell(q.model.getTerminal(c[0], !1)) : q.setSelectionCell(c[c.length - 1]), null != u.hoverIcons && u.hoverIcons.update(q.view.getState(q.getSelectionCell())), q.startEditingAtCell(q.getSelectionCell()), mxEvent.consume(b);
                    else if (mxEvent.isAltDown(b) && mxEvent.isShiftDown(b)) {
                        var d = C[b.keyCode];
                        null != d && (d.funct(b), mxEvent.consume(b))
                    } else 37 == b.keyCode ? (t(q.getSelectionCell(), mxConstants.DIRECTION_WEST), mxEvent.consume(b)) : 38 == b.keyCode ? (t(q.getSelectionCell(), mxConstants.DIRECTION_NORTH), mxEvent.consume(b)) : 39 == b.keyCode ? (t(q.getSelectionCell(), mxConstants.DIRECTION_EAST), mxEvent.consume(b)) : 40 == b.keyCode && (t(q.getSelectionCell(), mxConstants.DIRECTION_SOUTH), mxEvent.consume(b))
                }
            } catch (J) {
                u.handleError(J)
            }
            mxEvent.isConsumed(b) || D.apply(this, arguments)
        };
        var E = q.connectVertex;
        q.connectVertex = function(b, c, e, f, k, l, p) {
            var v = q.getIncomingEdges(b);
            if (a(b)) {
                var u = d(b),
                    t = u == mxConstants.DIRECTION_EAST || u == mxConstants.DIRECTION_WEST,
                    z = c == mxConstants.DIRECTION_EAST || c == mxConstants.DIRECTION_WEST;
                return u == c || 0 == v.length ? n(b, c) : t == z ? m(b) : g(b, c != mxConstants.DIRECTION_NORTH && c != mxConstants.DIRECTION_WEST)
            }
            return E.apply(this, arguments)
        };
        q.getSubtree = function(b) {
            var d = [b];
            !c(b) && !a(b) || l(b) || q.traverse(b, !0, function(a, b) {
                null != b && 0 > mxUtils.indexOf(d, b) && d.push(b);
                0 > mxUtils.indexOf(d, a) && d.push(a);
                return !0
            });
            return d
        };
        var F = mxVertexHandler.prototype.init;
        mxVertexHandler.prototype.init = function() {
            F.apply(this, arguments);
            (c(this.state.cell) || a(this.state.cell)) && !l(this.state.cell) && 0 < this.graph.getOutgoingEdges(this.state.cell).length && (this.moveHandle = mxUtils.createImage(Editor.moveImage), this.moveHandle.setAttribute("title", "Move Subtree"), this.moveHandle.style.position = "absolute", this.moveHandle.style.cursor = "pointer", this.moveHandle.style.width = "24px", this.moveHandle.style.height = "24px", this.graph.container.appendChild(this.moveHandle), mxEvent.addGestureListeners(this.moveHandle, mxUtils.bind(this, function(a) {
                this.graph.graphHandler.start(this.state.cell, mxEvent.getClientX(a), mxEvent.getClientY(a), this.graph.getSubtree(this.state.cell));
                this.graph.graphHandler.cellWasClicked = !0;
                this.graph.isMouseTrigger = mxEvent.isMouseEvent(a);
                this.graph.isMouseDown = !0;
                u.hoverIcons.reset();
                mxEvent.consume(a)
            })))
        };
        var H = mxVertexHandler.prototype.redrawHandles;
        mxVertexHandler.prototype.redrawHandles = function() {
            H.apply(this, arguments);
            null != this.moveHandle && (this.moveHandle.style.left = this.state.x + this.state.width + (40 > this.state.width ? 10 : 0) + 2 + "px", this.moveHandle.style.top = this.state.y + this.state.height + (40 > this.state.height ? 10 : 0) + 2 + "px")
        };
        var I = mxVertexHandler.prototype.setHandlesVisible;
        mxVertexHandler.prototype.setHandlesVisible = function(a) {
            I.apply(this, arguments);
            null != this.moveHandle && (this.moveHandle.style.display = a ? "" : "none")
        };
        var K = mxVertexHandler.prototype.destroy;
        mxVertexHandler.prototype.destroy = function(a, b) {
            K.apply(this, arguments);
            null != this.moveHandle && (this.moveHandle.parentNode.removeChild(this.moveHandle), this.moveHandle = null)
        }
    };
    if ("undefined" !== typeof Sidebar) {
        var c = Sidebar.prototype.createAdvancedShapes;
        Sidebar.prototype.createAdvancedShapes = function() {
            var a = c.apply(this, arguments),
                e = this.graph;
            return a.concat([this.addEntry("tree container", function() {
                var a = new mxCell("Tree Container", new mxGeometry(0, 0, 400, 320), "swimlane;html=1;startSize=20;horizontal=1;containerType=tree;");
                a.vertex = !0;
                var b = new mxCell("Parent", new mxGeometry(140, 60, 120, 40), 'whiteSpace=wrap;html=1;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                b.vertex = !0;
                var c = new mxCell("Child", new mxGeometry(140, 140, 120, 40), 'whiteSpace=wrap;html=1;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                c.vertex = !0;
                var e = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=elbowEdgeStyle;elbow=vertical;startArrow=none;endArrow=none;rounded=0;");
                e.geometry.relative = !0;
                e.edge = !0;
                b.insertEdge(e, !0);
                c.insertEdge(e, !1);
                a.insert(e);
                a.insert(b);
                a.insert(c);
                return sb.createVertexTemplateFromCells([a], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree mindmap mindmaps central idea branch topic", function() {
                var a = new mxCell("Mindmap", new mxGeometry(0, 0, 420, 126), "swimlane;html=1;startSize=20;horizontal=1;containerType=tree;");
                a.vertex = !0;
                var b = new mxCell("Central Idea", new mxGeometry(160, 60, 100, 40), 'ellipse;whiteSpace=wrap;html=1;align=center;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                b.vertex = !0;
                var c = new mxCell("Topic", new mxGeometry(320, 40, 80, 20), 'whiteSpace=wrap;html=1;rounded=1;arcSize=50;align=center;verticalAlign=middle;strokeWidth=1;autosize=1;spacing=4;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                c.vertex = !0;
                var e = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                e.geometry.relative = !0;
                e.edge = !0;
                b.insertEdge(e, !0);
                c.insertEdge(e, !1);
                var k = new mxCell("Branch", new mxGeometry(320, 80, 72, 26), 'whiteSpace=wrap;html=1;shape=partialRectangle;top=0;left=0;bottom=1;right=0;points=[[0,1],[1,1]];fillColor=none;align=center;verticalAlign=bottom;routingCenterY=0.5;snapToPoint=1;autosize=1;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                k.vertex = !0;
                var n = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                n.geometry.relative = !0;
                n.edge = !0;
                b.insertEdge(n, !0);
                k.insertEdge(n, !1);
                var p = new mxCell("Topic", new mxGeometry(20, 40, 80, 20), 'whiteSpace=wrap;html=1;rounded=1;arcSize=50;align=center;verticalAlign=middle;strokeWidth=1;autosize=1;spacing=4;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                p.vertex = !0;
                var t = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                t.geometry.relative = !0;
                t.edge = !0;
                b.insertEdge(t, !0);
                p.insertEdge(t, !1);
                var u = new mxCell("Branch", new mxGeometry(20, 80, 72, 26), 'whiteSpace=wrap;html=1;shape=partialRectangle;top=0;left=0;bottom=1;right=0;points=[[0,1],[1,1]];fillColor=none;align=center;verticalAlign=bottom;routingCenterY=0.5;snapToPoint=1;autosize=1;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                u.vertex = !0;
                var q = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                q.geometry.relative = !0;
                q.edge = !0;
                b.insertEdge(q, !0);
                u.insertEdge(q, !1);
                a.insert(e);
                a.insert(n);
                a.insert(t);
                a.insert(q);
                a.insert(b);
                a.insert(c);
                a.insert(k);
                a.insert(p);
                a.insert(u);
                return sb.createVertexTemplateFromCells([a], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree mindmap mindmaps central idea", function() {
                var a = new mxCell("Central Idea", new mxGeometry(0, 0, 100, 40), 'ellipse;whiteSpace=wrap;html=1;align=center;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};treeFolding=1;treeMoving=1;');
                a.vertex = !0;
                return sb.createVertexTemplateFromCells([a], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree mindmap mindmaps branch", function() {
                var a = new mxCell("Branch", new mxGeometry(0, 0, 80, 20), 'whiteSpace=wrap;html=1;shape=partialRectangle;top=0;left=0;bottom=1;right=0;points=[[0,1],[1,1]];fillColor=none;align=center;verticalAlign=bottom;routingCenterY=0.5;snapToPoint=1;recursiveResize=0;autosize=1;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                a.vertex = !0;
                var b = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                b.geometry.setTerminalPoint(new mxPoint(-40, 40), !0);
                b.geometry.relative = !0;
                b.edge = !0;
                a.insertEdge(b, !1);
                return sb.createVertexTemplateFromCells([a, b], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree mindmap mindmaps sub topic", function() {
                var a = new mxCell("Sub Topic", new mxGeometry(0, 0, 72, 26), 'whiteSpace=wrap;html=1;rounded=1;arcSize=50;align=center;verticalAlign=middle;strokeWidth=1;autosize=1;spacing=4;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"entityRelationEdgeStyle","startArrow":"none","endArrow":"none","segment":10,"curved":1};');
                a.vertex = !0;
                var b = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=entityRelationEdgeStyle;startArrow=none;endArrow=none;segment=10;curved=1;");
                b.geometry.setTerminalPoint(new mxPoint(-40, 40), !0);
                b.geometry.relative = !0;
                b.edge = !0;
                a.insertEdge(b, !1);
                return sb.createVertexTemplateFromCells([a, b], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree orgchart organization division", function() {
                var a = new mxCell("Orgchart", new mxGeometry(0, 0, 280, 220), 'swimlane;html=1;startSize=20;horizontal=1;containerType=tree;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                a.vertex = !0;
                var b = new mxCell("Organization", new mxGeometry(80, 40, 120, 60), 'whiteSpace=wrap;html=1;align=center;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                e.setAttributeForCell(b, "treeRoot", "1");
                b.vertex = !0;
                var c = new mxCell("Division", new mxGeometry(20, 140, 100, 60), 'whiteSpace=wrap;html=1;align=center;verticalAlign=middle;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                c.vertex = !0;
                var g = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=elbowEdgeStyle;elbow=vertical;startArrow=none;endArrow=none;rounded=0;");
                g.geometry.relative = !0;
                g.edge = !0;
                b.insertEdge(g, !0);
                c.insertEdge(g, !1);
                var k = new mxCell("Division", new mxGeometry(160, 140, 100, 60), 'whiteSpace=wrap;html=1;align=center;verticalAlign=middle;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                k.vertex = !0;
                var n = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=elbowEdgeStyle;elbow=vertical;startArrow=none;endArrow=none;rounded=0;");
                n.geometry.relative = !0;
                n.edge = !0;
                b.insertEdge(n, !0);
                k.insertEdge(n, !1);
                a.insert(g);
                a.insert(n);
                a.insert(b);
                a.insert(c);
                a.insert(k);
                return sb.createVertexTemplateFromCells([a], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree root", function() {
                var a = new mxCell("Organization", new mxGeometry(0, 0, 120, 60), 'whiteSpace=wrap;html=1;align=center;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                e.setAttributeForCell(a, "treeRoot", "1");
                a.vertex = !0;
                return sb.createVertexTemplateFromCells([a], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree division", function() {
                var a = new mxCell("Division", new mxGeometry(20, 40, 100, 60), 'whiteSpace=wrap;html=1;align=center;verticalAlign=middle;treeFolding=1;treeMoving=1;newEdgeStyle={"edgeStyle":"elbowEdgeStyle","startArrow":"none","endArrow":"none"};');
                a.vertex = !0;
                var b = new mxCell("", new mxGeometry(0, 0, 0, 0), "edgeStyle=elbowEdgeStyle;elbow=vertical;startArrow=none;endArrow=none;rounded=0;");
                b.geometry.setTerminalPoint(new mxPoint(0, 0), !0);
                b.geometry.relative = !0;
                b.edge = !0;
                a.insertEdge(b, !1);
                return sb.createVertexTemplateFromCells([a, b], a.geometry.width, a.geometry.height, a.value)
            }), this.addEntry("tree sub sections", function() {
                var a = new mxCell("Sub Section", new mxGeometry(0, 0, 100, 60), "whiteSpace=wrap;html=1;align=center;verticalAlign=middle;treeFolding=1;treeMoving=1;");
                a.vertex = !0;
                var b = new mxCell("", new mxGeometry(0, 0, 0, 0), "startArrow=none;endArrow=none;rounded=0;targetPortConstraint=eastwest;sourcePortConstraint=northsouth;");
                b.geometry.setTerminalPoint(new mxPoint(110, -40), !0);
                b.geometry.relative = !0;
                b.edge = !0;
                a.insertEdge(b, !1);
                var c = new mxCell("Sub Section", new mxGeometry(120, 0, 100, 60), "whiteSpace=wrap;html=1;align=center;verticalAlign=middle;treeFolding=1;treeMoving=1;");
                c.vertex = !0;
                var e = new mxCell("", new mxGeometry(0, 0, 0, 0), "startArrow=none;endArrow=none;rounded=0;targetPortConstraint=eastwest;sourcePortConstraint=northsouth;");
                e.geometry.setTerminalPoint(new mxPoint(110, -40), !0);
                e.geometry.relative = !0;
                e.edge = !0;
                c.insertEdge(e, !1);
                return sb.createVertexTemplateFromCells([b, e, a, c], 220, 60, "Sub Sections")
            })])
        }
    }
})();

//**Set Container in the same layer */

//*Right Sidebar */
/*for changing box color according to action type or layer */
(function() {
    "undefined" !== typeof html4 && (html4.ATTRIBS["span::data-lucid-content"] = 0, html4.ATTRIBS["span::data-lucid-type"] = 0);
    Editor.prototype.appName = "diagrams.net";
    Editor.prototype.diagramFileTypes = [{
        description: "diagramXmlDesc",
        extension: "drawio"
    }, {
        description: "diagramPngDesc",
        extension: "png"
    }, {
        description: "diagramSvgDesc",
        extension: "svg"
    }, {
        description: "diagramHtmlDesc",
        extension: "html"
    }, {
        description: "diagramXmlDesc",
        extension: "xml"
    }];
    Editor.prototype.libraryFileTypes = [{
        description: "Library (.drawiolib, .xml)",
        extensions: ["drawiolib", "xml"]
    }];
    Editor.prototype.fileExtensions = [{
        ext: "html",
        title: "filetypeHtml"
    }, {
        ext: "png",
        title: "filetypePng"
    }, {
        ext: "svg",
        title: "filetypeSvg"
    }];
    Editor.styles = [{}, {
        commonStyle: {
            fontColor: "#5C5C5C",
            strokeColor: "#006658",
            fillColor: "#21C0A5"
        }
    }, {
        commonStyle: {
            fontColor: "#095C86",
            strokeColor: "#AF45ED",
            fillColor: "#F694C1"
        },
        edgeStyle: {
            strokeColor: "#60E696"
        }
    }, {
        commonStyle: {
            fontColor: "#46495D",
            strokeColor: "#788AA3",
            fillColor: "#B2C9AB"
        }
    }, {
        commonStyle: {
            fontColor: "#5AA9E6",
            strokeColor: "#FF6392",
            fillColor: "#FFE45E"
        }
    }, {
        commonStyle: {
            fontColor: "#1D3557",
            strokeColor: "#457B9D",
            fillColor: "#A8DADC"
        },
        graph: {
            background: "#F1FAEE"
        }
    }, {
        commonStyle: {
            fontColor: "#393C56",
            strokeColor: "#E07A5F",
            fillColor: "#F2CC8F"
        },
        graph: {
            background: "#F4F1DE",
            gridColor: "#D4D0C0"
        }
    }, {
        commonStyle: {
            fontColor: "#143642",
            strokeColor: "#0F8B8D",
            fillColor: "#FAE5C7"
        },
        edgeStyle: {
            strokeColor: "#A8201A"
        },
        graph: {
            background: "#DAD2D8",
            gridColor: "#ABA4A9"
        }
    }, {
        commonStyle: {
            fontColor: "#FEFAE0",
            strokeColor: "#DDA15E",
            fillColor: "#BC6C25"
        },
        graph: {
            background: "#283618",
            gridColor: "#48632C"
        }
    }, {
        commonStyle: {
            fontColor: "#E4FDE1",
            strokeColor: "#028090",
            fillColor: "#F45B69"
        },
        graph: {
            background: "#114B5F",
            gridColor: "#0B3240"
        }
    }, {}, {
        vertexStyle: {
            strokeColor: "#D0CEE2",
            fillColor: "#FAD9D5"
        },
        edgeStyle: {
            strokeColor: "#09555B"
        },
        commonStyle: {
            fontColor: "#1A1A1A"
        }
    }, {
        vertexStyle: {
            strokeColor: "#BAC8D3",
            fillColor: "#09555B",
            fontColor: "#EEEEEE"
        },
        edgeStyle: {
            strokeColor: "#0B4D6A"
        }
    }, {
        vertexStyle: {
            strokeColor: "#D0CEE2",
            fillColor: "#5D7F99"
        },
        edgeStyle: {
            strokeColor: "#736CA8"
        },
        commonStyle: {
            fontColor: "#1A1A1A"
        }
    }, {
        vertexStyle: {
            strokeColor: "#FFFFFF",
            fillColor: "#182E3E",
            fontColor: "#FFFFFF"
        },
        edgeStyle: {
            strokeColor: "#23445D"
        },
        graph: {
            background: "#FCE7CD",
            gridColor: "#CFBDA8"
        }
    }, {
        vertexStyle: {
            strokeColor: "#FFFFFF",
            fillColor: "#F08E81"
        },
        edgeStyle: {
            strokeColor: "#182E3E"
        },
        commonStyle: {
            fontColor: "#1A1A1A"
        },
        graph: {
            background: "#B0E3E6",
            gridColor: "#87AEB0"
        }
    }, {
        vertexStyle: {
            strokeColor: "#909090",
            fillColor: "#F5AB50"
        },
        edgeStyle: {
            strokeColor: "#182E3E"
        },
        commonStyle: {
            fontColor: "#1A1A1A"
        },
        graph: {
            background: "#EEEEEE"
        }
    }, {
        vertexStyle: {
            strokeColor: "#EEEEEE",
            fillColor: "#56517E",
            fontColor: "#FFFFFF"
        },
        edgeStyle: {
            strokeColor: "#182E3E"
        },
        graph: {
            background: "#FAD9D5",
            gridColor: "#BFA6A3"
        }
    }, {
        vertexStyle: {
            strokeColor: "#BAC8D3",
            fillColor: "#B1DDF0",
            fontColor: "#182E3E"
        },
        edgeStyle: {
            strokeColor: "#EEEEEE",
            fontColor: "#FFFFFF"
        },
        graph: {
            background: "#09555B",
            gridColor: "#13B4C2"
        }
    }, {
        vertexStyle: {
            fillColor: "#EEEEEE",
            fontColor: "#1A1A1A"
        },
        edgeStyle: {
            fontColor: "#FFFFFF"
        },
        commonStyle: {
            strokeColor: "#FFFFFF"
        },
        graph: {
            background: "#182E3E",
            gridColor: "#4D94C7"
        }
    }];
    Editor.saveImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0iYmxhY2siIHdpZHRoPSIxOHB4IiBoZWlnaHQ9IjE4cHgiPjxwYXRoIGQ9Ik0wIDBoMjR2MjRIMHoiIGZpbGw9Im5vbmUiLz48cGF0aCBkPSJNMTkgMTJ2N0g1di03SDN2N2MwIDEuMS45IDIgMiAyaDE0YzEuMSAwIDItLjkgMi0ydi03aC0yem0tNiAuNjdsMi41OS0yLjU4TDE3IDExLjVsLTUgNS01LTUgMS40MS0xLjQxTDExIDEyLjY3VjNoMnoiLz48L3N2Zz4=";
    Editor.closeImage = mxClient.IS_SVG ? "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABkAAAAZCAMAAADzN3VRAAAApVBMVEUAAAD////k5OT///8AAAB1dXXMzMz9/f39/f37+/v5+fn+/v7///9iYmJaWlqFhYWnp6ejo6OHh4f////////////////7+/v5+fnx8fH///8AAAD///8bGxv7+/v5+fkoKCghISFDQ0MYGBjh4eHY2Njb29tQUFBvb29HR0c/Pz82NjYrKyu/v78SEhLu7u7s7OzV1dVVVVU7OzsVFRXAv78QEBBzqehMAAAAG3RSTlMAA/7p/vz5xZlrTiPL/v78+/v7+OXd2TYQDs8L70ZbAAABKUlEQVQoz3VS13LCMBBUXHChd8iukDslQChJ/v/TchaG4cXS+OSb1c7trU7V60OpdRz2ZtNZL4zXNlcN8BEtSG6+NxIXkeRPoBuQ1cjvZ31/VJFB10ISli6diYfH8iYO3WUNCcNlB0gTrXOtkxTo0O1aKKiBBMhhv2MNBQKoiA5wxlZo0JDzD3AYKbWacyj3fs01wxey0pyEP+R8pWKWXoqtIZ0DDg5pbki9krEKOa6LVDQsdoXEsi46Zqh69KFz7B1u7Hb2yDV8firXDKBlZ4UFiswKGRhXTS93/ECK7yxnJ3+S3y/ThpO+cfSD017nqa18aasabU0/t7d+tk0/1oMEJ1NaD67iwdF68OabFSLn+eHb0+vjy+uk8br9fdrftH0O2menfd7+AQfYM/lNjoDHAAAAAElFTkSuQmCC" : IMAGE_PATH + "/delete.png";
    Editor.plusImage = mxClient.IS_SVG ? "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAYAAACNMs+9AAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMC1jMDYwIDYxLjEzNDc3NywgMjAxMC8wMi8xMi0xNzozMjowMCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNSBNYWNpbnRvc2giIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6MDdCMTdENjVCOEM4MTFFNDlCRjVBNDdCODU5NjNBNUMiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6MDdCMTdENjZCOEM4MTFFNDlCRjVBNDdCODU5NjNBNUMiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDowN0IxN0Q2M0I4QzgxMUU0OUJGNUE0N0I4NTk2M0E1QyIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDowN0IxN0Q2NEI4QzgxMUU0OUJGNUE0N0I4NTk2M0E1QyIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PtjrjmgAAAAtSURBVHjaYvz//z8DMigvLwcLdHZ2MiKLMzEQCaivkLGsrOw/dU0cAr4GCDAARQsQbTFrv10AAAAASUVORK5CYII=" : IMAGE_PATH + "/plus.png";
    Editor.spinImage = mxClient.IS_SVG ? "data:image/gif;base64,R0lGODlhDAAMAPUxAEVriVp7lmCAmmGBm2OCnGmHn3OPpneSqYKbr4OcsIScsI2kto6kt46lt5KnuZmtvpquvpuvv56ywaCzwqK1xKu7yay9yq+/zLHAzbfF0bjG0bzJ1LzK1MDN18jT28nT3M3X3tHa4dTc49Xd5Njf5dng5t3k6d/l6uDm6uru8e7x8/Dz9fT29/b4+Pj5+fj5+vr6+v///wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACH5BAkKADEAIf8LTkVUU0NBUEUyLjADAQAAACwAAAAADAAMAAAGR8CYcEgsOgYAIax4CCQuQldrCBEsiK8VS2hoFGOrlJDA+cZQwkLnqyoJFZKviSS0ICrE0ec0jDAwIiUeGyBFGhMPFBkhZo1BACH5BAkKAC4ALAAAAAAMAAwAhVB0kFR3k1V4k2CAmmWEnW6Lo3KOpXeSqH2XrIOcsISdsImhtIqhtJCmuJGnuZuwv52wwJ+ywZ+ywqm6yLHBzbLCzrXEz7fF0LnH0rrI0r7L1b/M1sXR2cfT28rV3czW3s/Z4Nfe5Nvi6ODm6uLn6+Ln7OLo7OXq7efs7+zw8u/y9PDy9PX3+Pr7+////wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAZDQJdwSCxGDAIAoVFkFBwYSyIwGE4OkCJxIdG6WkJEx8sSKj7elfBB0a5SQg1EQ0SVVMPKhDM6iUIkRR4ZFxsgJl6JQQAh+QQJCgAxACwAAAAADAAMAIVGa4lcfZdjgpxkg51nhp5ui6N3kqh5lKqFnbGHn7KIoLOQp7iRp7mSqLmTqbqarr6br7+fssGitcOitcSuvsuuv8uwwMyzw861xNC5x9K6x9K/zNbDztjE0NnG0drJ1NzQ2eDS2+LT2+LV3ePZ4Oba4ebb4ufc4+jm6+7t8PLt8PPt8fPx8/Xx9PX09vf19/j3+Pn///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGQ8CYcEgsUhQFggFSjCQmnE1jcBhqGBXiIuAQSi7FGEIgfIzCFoCXFCZiPO0hKBMiwl7ET6eUYqlWLkUnISImKC1xbUEAIfkECQoAMgAsAAAAAAwADACFTnKPT3KPVHaTYoKcb4yjcY6leZSpf5mtgZuvh5+yiqG0i6K1jqW3kae5nrHBnrLBn7LCoLPCobTDqbrIqrvIs8LOtMPPtcPPtcTPuMbRucfSvcrUvsvVwMzWxdHaydTcytXdzNbezdff0drh2ODl2+Ln3eTp4Obq4ujs5Ont5uvu6O3w6u7w6u7x7/L09vj5+vr7+vv7////AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABkdAmXBILHIcicOCUqxELKKPxKAYgiYd4oMAEWo8RVmjIMScwhmBcJMKXwLCECmMGAhPI1QRwBiaSixCMDFhLSorLi8wYYxCQQAh+QQJCgAxACwAAAAADAAMAIVZepVggJphgZtnhp5vjKN2kah3kqmBmq+KobSLorWNpLaRp7mWq7ybr7+gs8KitcSktsWnuManucexwM2ywc63xtG6yNO9ytS+ytW/zNbDz9jH0tvL1d3N197S2+LU3OPU3ePV3eTX3+Xa4efb4ufd5Onl6u7r7vHs7/Lt8PLw8/Xy9Pby9fb09ff2+Pn3+Pn6+vr///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGSMCYcEgseiwSR+RS7GA4JFGF8RiWNiEiJTERgkjFGAQh/KTCGoJwpApnBkITKrwoCFWnFlEhaAxXLC9CBwAGRS4wQgELYY1CQQAh+QQJCgAzACwAAAAADAAMAIVMcI5SdZFhgZtti6JwjaR4k6mAma6Cm6+KobSLorWLo7WNo7aPpredsMCescGitMOitcSmuMaqu8ixwc2zws63xdC4xtG5x9K9ytXAzdfCztjF0NnF0drK1d3M1t7P2N/P2eDT2+LX3+Xe5Onh5+vi5+vj6Ozk6e3n7O/o7O/q7vHs7/Lt8PPu8fPx8/X3+Pn6+vv7+/v8/Pz///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGRcCZcEgsmkIbTOZTLIlGqZNnchm2SCgiJ6IRqljFmQUiXIVnoITQde4chC9Y+LEQxmTFRkFSNFAqDAMIRQoCAAEEDmeLQQAh+QQJCgAwACwAAAAADAAMAIVXeZRefplff5lhgZtph59yjqV2kaeAmq6FnbGFnrGLorWNpLaQp7mRqLmYrb2essGgs8Klt8apusitvcquv8u2xNC7yNO8ydS8ytTAzdfBzdfM1t7N197Q2eDU3OPX3+XZ4ObZ4ebc4+jf5erg5erg5uvp7fDu8fPv8vTz9fb09vf19/j3+Pn4+fn5+vr6+/v///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGRUCYcEgspkwjEKhUVJ1QsBNp0xm2VixiSOMRvlxFGAcTJook5eEHIhQcwpWIkAFQECkNy9AQWFwyEAkPRQ4FAwQIE2llQQAh+QQJCgAvACwAAAAADAAMAIVNcY5SdZFigptph6BvjKN0kKd8lquAmq+EnbGGn7KHn7ONpLaOpbearr+csMCdscCescGhtMOnuMauvsuzws60w862xdC9ytW/y9a/zNbCztjG0drH0tvK1N3M1t7N19/U3ePb4uff5urj6Ozk6e3l6u7m6u7o7PDq7vDt8PPv8vTw8vTw8/X19vf6+vv///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGQ8CXcEgsvlytVUplJLJIpSEDUESFTELBwSgCCQEV42kjDFiMo4uQsDB2MkLHoEHUTD7DRAHC8VAiZ0QSCgYIDxhNiUEAOw==" : IMAGE_PATH + "/spin.gif";
    Editor.globeImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMTEuOTkgMkM2LjQ3IDIgMiA2LjQ4IDIgMTJzNC40NyAxMCA5Ljk5IDEwQzE3LjUyIDIyIDIyIDE3LjUyIDIyIDEyUzE3LjUyIDIgMTEuOTkgMnptNi45MyA2aC0yLjk1Yy0uMzItMS4yNS0uNzgtMi40NS0xLjM4LTMuNTYgMS44NC42MyAzLjM3IDEuOTEgNC4zMyAzLjU2ek0xMiA0LjA0Yy44MyAxLjIgMS40OCAyLjUzIDEuOTEgMy45NmgtMy44MmMuNDMtMS40MyAxLjA4LTIuNzYgMS45MS0zLjk2ek00LjI2IDE0QzQuMSAxMy4zNiA0IDEyLjY5IDQgMTJzLjEtMS4zNi4yNi0yaDMuMzhjLS4wOC42Ni0uMTQgMS4zMi0uMTQgMiAwIC42OC4wNiAxLjM0LjE0IDJINC4yNnptLjgyIDJoMi45NWMuMzIgMS4yNS43OCAyLjQ1IDEuMzggMy41Ni0xLjg0LS42My0zLjM3LTEuOS00LjMzLTMuNTZ6bTIuOTUtOEg1LjA4Yy45Ni0xLjY2IDIuNDktMi45MyA0LjMzLTMuNTZDOC44MSA1LjU1IDguMzUgNi43NSA4LjAzIDh6TTEyIDE5Ljk2Yy0uODMtMS4yLTEuNDgtMi41My0xLjkxLTMuOTZoMy44MmMtLjQzIDEuNDMtMS4wOCAyLjc2LTEuOTEgMy45NnpNMTQuMzQgMTRIOS42NmMtLjA5LS42Ni0uMTYtMS4zMi0uMTYtMiAwLS42OC4wNy0xLjM1LjE2LTJoNC42OGMuMDkuNjUuMTYgMS4zMi4xNiAyIDAgLjY4LS4wNyAxLjM0LS4xNiAyem0uMjUgNS41NmMuNi0xLjExIDEuMDYtMi4zMSAxLjM4LTMuNTZoMi45NWMtLjk2IDEuNjUtMi40OSAyLjkzLTQuMzMgMy41NnpNMTYuMzYgMTRjLjA4LS42Ni4xNC0xLjMyLjE0LTIgMC0uNjgtLjA2LTEuMzQtLjE0LTJoMy4zOGMuMTYuNjQuMjYgMS4zMS4yNiAycy0uMSAxLjM2LS4yNiAyaC0zLjM4eiIvPjwvc3ZnPg==";
    Editor.commentImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMjEuOTkgNGMwLTEuMS0uODktMi0xLjk5LTJINGMtMS4xIDAtMiAuOS0yIDJ2MTJjMCAxLjEuOSAyIDIgMmgxNGw0IDQtLjAxLTE4ek0xOCAxNEg2di0yaDEydjJ6bTAtM0g2VjloMTJ2MnptMC0zSDZWNmgxMnYyeiIvPjxwYXRoIGQ9Ik0wIDBoMjR2MjRIMHoiIGZpbGw9Im5vbmUiLz48L3N2Zz4=";
    Editor.commentImageInverted = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAABLElEQVRYR+2Wvy4FQRjFf4dINAq9XqtSaVRqXArPINEodUqlhxC5/pU6nYdQSHQeQTRHNtmVuXuXrIxv1k1sN5vMOb85c75kxMCfBvZnCsD2ErAGzAfAvUt6THUnAGwfAWfAQoB5I3kh6aBZfALYXgGeg80b31VJT9UiBRgB48CTp9Lbku7aAPvAZSGAHUm3swEgKWtUbbsj1f4JDA4AbGb24iErgUzzr7bvSrrpVcKgK5ghgKAO9E/gvwNBJRxJuu41BUEd+BFARA3+JsAWcB9x3A7NzgSqt+ALsFwAYhqgMrW9Ub8J14G5QJBugAhD2yfAaUt7T9LVxBhGmDeato/rZJtfZQHq600hygPUEIfAOTAMQALxWrQD7X7ZXpT0VqyE3xU868n9G5PzASPvpiHavBAUAAAAAElFTkSuQmCC";
    Editor.userImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMTIgMTJjMi4yMSAwIDQtMS43OSA0LTRzLTEuNzktNC00LTQtNCAxLjc5LTQgNCAxLjc5IDQgNCA0em0wIDJjLTIuNjcgMC04IDEuMzQtOCA0djJoMTZ2LTJjMC0yLjY2LTUuMzMtNC04LTR6Ii8+PC9zdmc+";
    Editor.shareImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMTggMTYuMDhjLS43NiAwLTEuNDQuMy0xLjk2Ljc3TDguOTEgMTIuN2MuMDUtLjIzLjA5LS40Ni4wOS0uN3MtLjA0LS40Ny0uMDktLjdsNy4wNS00LjExYy41NC41IDEuMjUuODEgMi4wNC44MSAxLjY2IDAgMy0xLjM0IDMtM3MtMS4zNC0zLTMtMy0zIDEuMzQtMyAzYzAgLjI0LjA0LjQ3LjA5LjdMOC4wNCA5LjgxQzcuNSA5LjMxIDYuNzkgOSA2IDljLTEuNjYgMC0zIDEuMzQtMyAzczEuMzQgMyAzIDNjLjc5IDAgMS41LS4zMSAyLjA0LS44MWw3LjEyIDQuMTZjLS4wNS4yMS0uMDguNDMtLjA4LjY1IDAgMS42MSAxLjMxIDIuOTIgMi45MiAyLjkyIDEuNjEgMCAyLjkyLTEuMzEgMi45Mi0yLjkycy0xLjMxLTIuOTItMi45Mi0yLjkyeiIvPjwvc3ZnPg==";
    Editor.syncImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMTIgNFYxTDggNWw0IDRWNmMzLjMxIDAgNiAyLjY5IDYgNiAwIDEuMDEtLjI1IDEuOTctLjcgMi44bDEuNDYgMS40NkMxOS41NCAxNS4wMyAyMCAxMy41NyAyMCAxMmMwLTQuNDItMy41OC04LTgtOHptMCAxNGMtMy4zMSAwLTYtMi42OS02LTYgMC0xLjAxLjI1LTEuOTcuNy0yLjhMNS4yNCA3Ljc0QzQuNDYgOC45NyA0IDEwLjQzIDQgMTJjMCA0LjQyIDMuNTggOCA4IDh2M2w0LTQtNC00djN6Ii8+PC9zdmc+";
    Editor.syncDisabledImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMTAgNi4zNVY0LjI2Yy0uOC4yMS0xLjU1LjU0LTIuMjMuOTZsMS40NiAxLjQ2Yy4yNS0uMTIuNS0uMjQuNzctLjMzem0tNy4xNC0uOTRsMi4zNiAyLjM2QzQuNDUgOC45OSA0IDEwLjQ0IDQgMTJjMCAyLjIxLjkxIDQuMiAyLjM2IDUuNjRMNCAyMGg2di02bC0yLjI0IDIuMjRDNi42OCAxNS4xNSA2IDEzLjY2IDYgMTJjMC0xIC4yNS0xLjk0LjY4LTIuNzdsOC4wOCA4LjA4Yy0uMjUuMTMtLjUuMjUtLjc3LjM0djIuMDljLjgtLjIxIDEuNTUtLjU0IDIuMjMtLjk2bDIuMzYgMi4zNiAxLjI3LTEuMjdMNC4xNCA0LjE0IDIuODYgNS40MXpNMjAgNGgtNnY2bDIuMjQtMi4yNEMxNy4zMiA4Ljg1IDE4IDEwLjM0IDE4IDEyYzAgMS0uMjUgMS45NC0uNjggMi43N2wxLjQ2IDEuNDZDMTkuNTUgMTUuMDEgMjAgMTMuNTYgMjAgMTJjMC0yLjIxLS45MS00LjItMi4zNi01LjY0TDIwIDR6Ii8+PC9zdmc+";
    Editor.syncProblemImage = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij48cGF0aCBkPSJNMyAxMmMwIDIuMjEuOTEgNC4yIDIuMzYgNS42NEwzIDIwaDZ2LTZsLTIuMjQgMi4yNEM1LjY4IDE1LjE1IDUgMTMuNjYgNSAxMmMwLTIuNjEgMS42Ny00LjgzIDQtNS42NVY0LjI2QzUuNTUgNS4xNSAzIDguMjcgMyAxMnptOCA1aDJ2LTJoLTJ2MnpNMjEgNGgtNnY2bDIuMjQtMi4yNEMxOC4zMiA4Ljg1IDE5IDEwLjM0IDE5IDEyYzAgMi42MS0xLjY3IDQuODMtNCA1LjY1djIuMDljMy40NS0uODkgNi00LjAxIDYtNy43NCAwLTIuMjEtLjkxLTQuMi0yLjM2LTUuNjRMMjEgNHptLTEwIDloMlY3aC0ydjZ6Ii8+PC9zdmc+";
    Editor.tweetImage = IMAGE_PATH + "/tweet.png";
    Editor.facebookImage = IMAGE_PATH + "/facebook.png";
    Editor.blankImage = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg==";
    Editor.hiResImage = mxClient.IS_SVG ? "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFAAAAA+CAMAAACLMWy1AAAAh1BMVEUAAABMTExERERBQUFBQUFFRUVAQEBCQkJAQEA6OjpDQ0NKSkpBQUFBQUFERERERERBQUFCQkJCQkJCQkJJSUlBQUFCQkJDQ0NDQ0NCQkJDQ0NBQUFBQUFCQkJBQUFCQkJCQkJDQ0NCQkJHR0dBQUFCQkJCQkJAQEBCQkJDQ0NAQEBERERCQkIk1hS2AAAAKnRSTlMAAjj96BL7PgQFRwfu3TYazKuVjRXl1V1DPCn1uLGjnWNVIgy9hU40eGqPkM38AAACG0lEQVRYw+2X63KbMBCFzwZblgGDceN74muatpLe//m6MHV3gHGFAv2RjM94MAbxzdnVsQbBDKwH8AH8MDAyafzjqYeyOG04XE7RS8nIRDXg6BlT+rA0nmtAPh+NQRDxIASIMG44rAMrGunBgHwy3uUldxggIStGKp2f+DQc2O4h4eQsX3O2IFB/oEbsjOKbStnjAEA+zJ0ylZTbgvoDn8xNyn6Dbj5Kd4GsNpABa6duQPfSdEj88TgMAhKuCWjAkgmFXPLnsD0pWd3OFGdrMugQII/eOMPEiGOzqPMIeWrcSoMCg71W1pXBPvCP+gS/OdXqQ3uW23+93XGWLl/OaBb805bNcBPoEIcVJsnHzcxpZH86u5KZ9gDby5dQCcnKqdbke4ItI4Tzd7IW9hZQt4EO6GG9b9sYuuK9Wwn8TIr2xKbF2+3Nhr+qxChJ/AI6pIfCu4z4Zowp4ZUNihz79vewzctnHDwTvQO/hCdFBzrUGDOPn2Y/F8YKT4oOATLvlhOznzmBSdFBJWtc58y7r+UVFOCQczy3wpN6pegDqHtsCPTGvH9JuTO0Dyg8icldYPk+RB6g8Aofj4m2EKBvtTmUPD9xDd1pPcSReV2U5iD/ik2yrngtvvqBfPzOvKiDTKTsCdoHZJ7pLLffgTwlJ5vJdtJV2/jiAYaLvLGhMAEDO5QcDg2M/jOw/8Zn+K3ZwJvHT7ZffgC/NvA3zcybTeIfE4EAAAAASUVORK5CYII=" : IMAGE_PATH + "/img-hi-res.png";
    Editor.loResImage = mxClient.IS_SVG ? "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFAAAAA+CAMAAACLMWy1AAAAS1BMVEVAQEAAAAA1NTVBQUFDQ0NDQ0NFRUVERERBQUFBQUFBQUFAQEBBQUFBQUFCQkJCQkJCQkJBQUFCQkJDQ0NDQ0NCQkJCQkJCQkJGRkb5/XqTAAAAGXRSTlP+AAWODlASCsesX+Lc2LyWe3pwa1tCPjohjSJfoAAAAI1JREFUWMPt1MkKhTAMRuG0anvneXr/J71nUypKcdqI/N8yhLMKMZE1CahnClDQzMPB44ED3EgeCubgDWnWQMHpwTtKwTe+UHD4sJ94wbUEHHFGhILlYDeSnsQeabeCgsPBgB0MOZZ9oGA5GJFiJSfUULAfjLjARrhCwX7wh2YCDwVbwZkUBKqFFJRN+wOcwSgR2sREcgAAAABJRU5ErkJggg==" : IMAGE_PATH + "/img-lo-res.png";
    Editor.cameraLargeImage = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAAlwSFlzAAALEwAACxMBAJqcGAAAAVlpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IlhNUCBDb3JlIDUuNC4wIj4KICAgPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4KICAgICAgPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIKICAgICAgICAgICAgeG1sbnM6dGlmZj0iaHR0cDovL25zLmFkb2JlLmNvbS90aWZmLzEuMC8iPgogICAgICAgICA8dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPgogICAgICA8L3JkZjpEZXNjcmlwdGlvbj4KICAgPC9yZGY6UkRGPgo8L3g6eG1wbWV0YT4KTMInWQAAA/BJREFUWAnFl0uIjWEYx885buPSuGwmSYwtwsY1ikKSNYNclmQnadgrZSPlsnBLSlaGBdNYKY0Vdi4L4zYzIqxGxmXG//d+7//0+uY7nWMiT/2/53mf+3v7vnNKpf9M5UbrDw8Pj4m+wzmeT1FBUS6Xf+YNox6reMONukijMXUTM3NmI75PyXcJPwRWg5kS7xysDLNmfEUxpx2rceNE50IlYjyRklcLf0prY+x4BTqfmx3ZUHQaO9ISGngYq38V/1EH+ECPa+QaK1u1kVBQirDMChiS3CTeIkwWvghtwhKBpZ8g1CO2B99FynVU/KowSRgQ3mlrBsVZ1awmQlS0SGbfXglfBPbdRGMm5O8RXg2P835pDCvzWjghTHETcLpZLHwS8kTCtBEK1SN83Egam8YxyVZqc+Do5qkwS+gT9grNwkUBG6cbsG/gs3BTuC/0ChCxq4QtwgzBMdwUZBPyN4Ftfi4sYPZHktbOSRlIuutRP5jYj0ueZp88xyYcS/zZoiLyQT1IA/cTj7eSlwnrhI+JnkQbCwo2Sx/2M7VJt17wdhVtgxvrpoFnAuSAbJQ97biZAlKxBfD9wgOhV+BgIR/AZtJ4kwD5PGSj7OmmekjWEy0oAQHAS3+KpBpzXqYK3UItopHpSRMno2N+cm7gDYnfRCcr3QBqriMHLJDkeyhFfiG5aVbK+8rhtP9M6QcIEJHX5Fp9NMAyQlYiu+OOJNlODCIXyka/P23bncTdiC7OydC1+v1Bsb+5r84DK8S3Rdmf5cRUFW3bXtWUSt1Rdk6G4SyJV2o1YId+vNUxr+x5yCJiapFtcxQzLjrxboGcMxvFJwEOKnLwjIbkx/sdSmeSaUY++SwTAxV+4DJT7RVwkbk46gNCsifIItuy0e9PF33Cb4homhN5YRyzL5q5V2VNkv98kqgoGTo3YF9CnMM5Y5rItFfvBSi9JulVXOgI+VwIntkt+SaZ6weQfcovJf7zpTfl86P/wAF7Fz18NeKwmvAWCaX0Z/uMHQr42ZxvR/Rxcw5xM+9J/CJq8w2gduDhmDgso/QrBH47dEXQ1IqczyHpIOfIRtnTtV7SwO1oKXKkU3fbToFGSDHtMWcaH1WBuVYnDbRFi99iqSMySdzxXckrazUh23KBVYGIcfNBkTxca0e4ATJ0KukGYVBgr/MnlhPOtQq/ksUfCbzh+EFCjtnCUoHfjhA/OsiTv2HcEvJMELp0VakZDliTmriTdPivxU4VmEhtPrGV+KJhO7ZKt0doFZh1fgZSBWIW2AGEHwg3BUWOnKtH+suqdw07tYMfglCrWPD5mw9qVYuniaXkT0OtWaSuo5LJTY1RBf+roF9X5+y/5qU+DAAAAABJRU5ErkJggg==";
    Editor.svgBrokenImage = Graph.createSvgImage(10, 10, '<rect x="0" y="0" width="10" height="10" stroke="#000" fill="transparent"/><path d="m 0 0 L 10 10 L 0 10 L 10 0" stroke="#000" fill="transparent"/>');
    Editor.defaultCustomLibraries = [];
    Editor.enableCustomLibraries = !0;
    Editor.enableCustomProperties = !0;
    Editor.compressXml = !0;
    Editor.globalVars = null;
    Editor.shadowOptionEnabled = !mxClient.IS_SF;
    Editor.config = null;
    Editor.configVersion = null;
    Editor.defaultBorder = 5;
    Editor.commonProperties = [{
        name: "comic",
        dispName: "Comic",
        type: "bool",
        defVal: !1,
        isVisible: function(a, d) {
            return "1" != mxUtils.getValue(a.style, "sketch", "0")
        }
    }, {
        name: "jiggle",
        dispName: "Jiggle",
        type: "float",
        min: 0,
        defVal: 1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "comic", "0") || "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "fillWeight",
        dispName: "Fill Weight",
        type: "int",
        defVal: -1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "hachureGap",
        dispName: "Hachure Gap",
        type: "int",
        defVal: -1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "hachureAngle",
        dispName: "Hachure Angle",
        type: "int",
        defVal: -41,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "curveFitting",
        dispName: "Curve Fitting",
        type: "float",
        defVal: .95,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "simplification",
        dispName: "Simplification",
        type: "float",
        defVal: 0,
        min: 0,
        max: 1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "disableMultiStroke",
        dispName: "Disable Multi Stroke",
        type: "bool",
        defVal: !1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "disableMultiStrokeFill",
        dispName: "Disable Multi Stroke Fill",
        type: "bool",
        defVal: !1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "dashOffset",
        dispName: "Dash Offset",
        type: "int",
        defVal: -1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "dashGap",
        dispName: "Dash Gap",
        type: "int",
        defVal: -1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "zigzagOffset",
        dispName: "ZigZag Offset",
        type: "int",
        defVal: -1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "jiggle",
        dispName: "Jiggle",
        type: "float",
        min: 0,
        defVal: 1,
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "comic", "0") || "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }, {
        name: "sketchStyle",
        dispName: "Sketch Style",
        type: "enum",
        defVal: "rough",
        enumList: [{
            val: "rough",
            dispName: "Rough"
        }, {
            val: "comic",
            dispName: "Comic"
        }],
        isVisible: function(a, d) {
            return "1" == mxUtils.getValue(a.style, "sketch", "1" == urlParams.rough ? "1" : "0")
        }
    }];

    /*Arc Property */
    Editor.commonEdgeProperties = [{
            type: "separator"
        },
        {
            name: "seeAlso",
            dispName: "See Also",
            type: "char",
        },
        {
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        },
        //  {
        //   name: "timeStamp",
        //   dispName: "Time Stamp",
        //   type: "char",
        //   defVal: "YYYY-MM-DD HH:MM:SS"
        // }, 
        {
            name: "sourcePortConstraint",
            dispName: "Source Constraint",
            type: "enum",
            defVal: "none",
            enumList: [{
                val: "none",
                dispName: "None"
            }, {
                val: "north",
                dispName: "North"
            }, {
                val: "east",
                dispName: "East"
            }, {
                val: "south",
                dispName: "South"
            }, {
                val: "west",
                dispName: "West"
            }]
        }, {
            name: "targetPortConstraint",
            dispName: "Target Constraint",
            type: "enum",
            defVal: "none",
            enumList: [{
                val: "none",
                dispName: "None"
            }, {
                val: "north",
                dispName: "North"
            }, {
                val: "east",
                dispName: "East"
            }, {
                val: "south",
                dispName: "South"
            }, {
                val: "west",
                dispName: "West"
            }]
        }, {
            name: "jettySize",
            dispName: "Jetty Size",
            type: "int",
            min: 0,
            defVal: "auto",
            allowAuto: !0,
            isVisible: function(a) {
                return "orthogonalEdgeStyle" == mxUtils.getValue(a.style, mxConstants.STYLE_EDGE, null)
            }
        }, {
            name: "fillOpacity",
            dispName: "Fill Opacity",
            type: "int",
            min: 0,
            max: 100,
            defVal: 100
        }, {
            name: "strokeOpacity",
            dispName: "Stroke Opacity",
            type: "int",
            min: 0,
            max: 100,
            defVal: 100
        }, {
            name: "startFill",
            dispName: "Start Fill",
            type: "bool",
            defVal: !0
        }, {
            name: "endFill",
            dispName: "End Fill",
            type: "bool",
            defVal: !0
        }, {
            name: "perimeterSpacing",
            dispName: "Terminal Spacing",
            type: "float",
            defVal: 0
        }, {
            name: "anchorPointDirection",
            dispName: "Anchor Direction",
            type: "bool",
            defVal: !0
        }, {
            name: "snapToPoint",
            dispName: "Snap to Point",
            type: "bool",
            defVal: !1
        }, {
            name: "fixDash",
            dispName: "Fixed Dash",
            type: "bool",
            defVal: !1
        }, {
            name: "editable",
            dispName: "Editable",
            type: "bool",
            defVal: !0
        }, {
            name: "metaEdit",
            dispName: "Edit Dialog",
            type: "bool",
            defVal: !1
        }, {
            name: "backgroundOutline",
            dispName: "Background Outline",
            type: "bool",
            defVal: !1
        }, {
            name: "bendable",
            dispName: "Bendable",
            type: "bool",
            defVal: !0
        }, {
            name: "movable",
            dispName: "Movable",
            type: "bool",
            defVal: !0
        }, {
            name: "cloneable",
            dispName: "Cloneable",
            type: "bool",
            defVal: !0
        }, {
            name: "deletable",
            dispName: "Deletable",
            type: "bool",
            defVal: !0
        }, {
            name: "orthogonalLoop",
            dispName: "Loop Routing",
            type: "bool",
            defVal: !1
        }, {
            name: "noJump",
            dispName: "No Jumps",
            type: "bool",
            defVal: !1
        }
    ].concat(Editor.commonProperties);

    /*Node Property */
    Editor.commonVertexProperties = [{
            type: "separator"
        },
        {
            name: "resizeLastRow",
            dispName: "Resize Last Row",
            type: "bool",
            getDefaultValue: function(a, d) {
                var b = d.editorUi.editor.graph.getCellStyle(1 == a.vertices.length && 0 == a.edges.length ? a.vertices[0] : null);
                return "1" == mxUtils.getValue(b, "resizeLastRow", "0")
            },
            isVisible: function(a, d) {
                var b = d.editorUi.editor.graph;
                return 1 == a.vertices.length && 0 == a.edges.length && b.isTable(a.vertices[0])
            }
        }, {
            name: "resizeLast",
            dispName: "Resize Last Column",
            type: "bool",
            getDefaultValue: function(a, d) {
                var b = d.editorUi.editor.graph.getCellStyle(1 == a.vertices.length && 0 == a.edges.length ? a.vertices[0] : null);
                return "1" == mxUtils.getValue(b, "resizeLast", "0")
            },
            isVisible: function(a, d) {
                var b = d.editorUi.editor.graph;
                return 1 == a.vertices.length && 0 == a.edges.length && b.isTable(a.vertices[0])
            }
        },
        {
            name: "fillOpacity",
            dispName: "Fill Opacity",
            type: "int",
            min: 0,
            max: 100,
            defVal: 100
        }, {
            name: "strokeOpacity",
            dispName: "Stroke Opacity",
            type: "int",
            min: 0,
            max: 100,
            defVal: 100
        }, {
            name: "overflow",
            dispName: "Text Overflow",
            defVal: "visible",
            type: "enum",
            enumList: [{
                val: "visible",
                dispName: "Visible"
            }, {
                val: "hidden",
                dispName: "Hidden"
            }, {
                val: "fill",
                dispName: "Fill"
            }, {
                val: "width",
                dispName: "Width"
            }]
        }, {
            name: "noLabel",
            dispName: "Hide Label",
            type: "bool",
            defVal: !1
        }, {
            name: "labelPadding",
            dispName: "Label Padding",
            type: "float",
            defVal: 0
        }, {
            name: "direction",
            dispName: "Direction",
            type: "enum",
            defVal: "east",
            enumList: [{
                val: "north",
                dispName: "North"
            }, {
                val: "east",
                dispName: "East"
            }, {
                val: "south",
                dispName: "South"
            }, {
                val: "west",
                dispName: "West"
            }]
        }, {
            name: "portConstraint",
            dispName: "Constraint",
            type: "enum",
            defVal: "none",
            enumList: [{
                val: "none",
                dispName: "None"
            }, {
                val: "north",
                dispName: "North"
            }, {
                val: "east",
                dispName: "East"
            }, {
                val: "south",
                dispName: "South"
            }, {
                val: "west",
                dispName: "West"
            }]
        }, {
            name: "portConstraintRotation",
            dispName: "Rotate Constraint",
            type: "bool",
            defVal: !1
        }, {
            name: "connectable",
            dispName: "Connectable",
            type: "bool",
            getDefaultValue: function(a, d) {
                return d.editorUi.editor.graph.isCellConnectable(1 == a.vertices.length && 0 == a.edges.length ? a.vertices[0] : null)
            },
            isVisible: function(a, d) {
                return 1 == a.vertices.length && 0 == a.edges.length
            }
        }, {
            name: "allowArrows",
            dispName: "Allow Arrows",
            type: "bool",
            defVal: !0
        }, {
            name: "snapToPoint",
            dispName: "Snap to Point",
            type: "bool",
            defVal: !1
        }, {
            name: "perimeter",
            dispName: "Perimeter",
            defVal: "none",
            type: "enum",
            enumList: [{
                val: "none",
                dispName: "None"
            }, {
                val: "rectanglePerimeter",
                dispName: "Rectangle"
            }, {
                val: "ellipsePerimeter",
                dispName: "Ellipse"
            }, {
                val: "rhombusPerimeter",
                dispName: "Rhombus"
            }, {
                val: "trianglePerimeter",
                dispName: "Triangle"
            }, {
                val: "hexagonPerimeter2",
                dispName: "Hexagon"
            }, {
                val: "lifelinePerimeter",
                dispName: "Lifeline"
            }, {
                val: "orthogonalPerimeter",
                dispName: "Orthogonal"
            }, {
                val: "backbonePerimeter",
                dispName: "Backbone"
            }, {
                val: "calloutPerimeter",
                dispName: "Callout"
            }, {
                val: "parallelogramPerimeter",
                dispName: "Parallelogram"
            }, {
                val: "trapezoidPerimeter",
                dispName: "Trapezoid"
            }, {
                val: "stepPerimeter",
                dispName: "Step"
            }]
        }, {
            name: "fixDash",
            dispName: "Fixed Dash",
            type: "bool",
            defVal: !1
        }, {
            name: "autosize",
            dispName: "Autosize",
            type: "bool",
            defVal: !1
        }, {
            name: "container",
            dispName: "Container",
            type: "bool",
            defVal: !1,
            isVisible: function(a, d) {
                return 1 == a.vertices.length && 0 == a.edges.length
            }
        }, {
            name: "dropTarget",
            dispName: "Drop Target",
            type: "bool",
            getDefaultValue: function(a, d) {
                var b = 1 == a.vertices.length && 0 == a.edges.length ? a.vertices[0] : null,
                    c = d.editorUi.editor.graph;
                return null != b && (c.isSwimlane(b) || 0 < c.model.getChildCount(b))
            },
            isVisible: function(a, d) {
                return 1 == a.vertices.length && 0 == a.edges.length
            }
        }, {
            name: "collapsible",
            dispName: "Collapsible",
            type: "bool",
            getDefaultValue: function(a, d) {
                var b = 1 == a.vertices.length && 0 == a.edges.length ? a.vertices[0] : null,
                    c = d.editorUi.editor.graph;
                return null != b && (c.isContainer(b) && "0" != a.style.collapsible || !c.isContainer(b) && "1" == a.style.collapsible)
            },
            isVisible: function(a, d) {
                return 1 == a.vertices.length && 0 == a.edges.length
            }
        }, {
            name: "recursiveResize",
            dispName: "Resize Children",
            type: "bool",
            defVal: !0,
            isVisible: function(a, d) {
                return 1 == a.vertices.length && 0 == a.edges.length && !d.editorUi.editor.graph.isSwimlane(a.vertices[0]) && null == mxUtils.getValue(a.style, "childLayout", null)
            }
        }, {
            name: "expand",
            dispName: "Expand",
            type: "bool",
            defVal: !0
        }, {
            name: "part",
            dispName: "Part",
            type: "bool",
            defVal: !1,
            isVisible: function(a, d) {
                var b = d.editorUi.editor.graph.model;
                return 0 < a.vertices.length ? b.isVertex(b.getParent(a.vertices[0])) : !1
            }
        }, {
            name: "editable",
            dispName: "Editable",
            type: "bool",
            defVal: !0
        }, {
            name: "metaEdit",
            dispName: "Edit Dialog",
            type: "bool",
            defVal: !1
        }, {
            name: "backgroundOutline",
            dispName: "Background Outline",
            type: "bool",
            defVal: !1
        }, {
            name: "movable",
            dispName: "Movable",
            type: "bool",
            defVal: !0
        }, {
            name: "movableLabel",
            dispName: "Movable Label",
            type: "bool",
            defVal: !1,
            isVisible: function(a, d) {
                var b = 0 < a.vertices.length ? d.editorUi.editor.graph.getCellGeometry(a.vertices[0]) : null;
                return null != b && !b.relative
            }
        }, {
            name: "resizable",
            dispName: "Resizable",
            type: "bool",
            defVal: !0
        }, {
            name: "resizeWidth",
            dispName: "Resize Width",
            type: "bool",
            defVal: !1
        }, {
            name: "resizeHeight",
            dispName: "Resize Height",
            type: "bool",
            defVal: !1
        }, {
            name: "rotatable",
            dispName: "Rotatable",
            type: "bool",
            defVal: !0
        }, {
            name: "cloneable",
            dispName: "Cloneable",
            type: "bool",
            defVal: !0
        }, {
            name: "deletable",
            dispName: "Deletable",
            type: "bool",
            defVal: !0
        }, {
            name: "treeFolding",
            dispName: "Tree Folding",
            type: "bool",
            defVal: !1
        }, {
            name: "treeMoving",
            dispName: "Tree Moving",
            type: "bool",
            defVal: !1
        }, {
            name: "pointerEvents",
            dispName: "Pointer Events",
            type: "bool",
            defVal: !0,
            isVisible: function(a, d) {
                var b = mxUtils.getValue(a.style, mxConstants.STYLE_FILLCOLOR, null);
                return d.editorUi.editor.graph.isSwimlane(a.vertices[0]) || null == b || b == mxConstants.NONE
            }
        }, {
            name: "moveCells",
            dispName: "Move Cells on Fold",
            type: "bool",
            defVal: !1,
            isVisible: function(a, d) {
                return 0 < a.vertices.length && d.editorUi.editor.graph.isContainer(a.vertices[0])
            }
        }
    ].concat(Editor.commonProperties);




    Editor.defaultCsvValue = '##\n## Example CSV import. Use ## for comments and # for configuration. Paste CSV below.\n## The following names are reserved and should not be used (or ignored):\n## id, tooltip, placeholder(s), link and label (see below)\n##\n#\n## Node label with placeholders and HTML.\n## Default is \'%name_of_first_column%\'.\n#\n# label: %name%<br><i style="color:gray;">%position%</i><br><a href="mailto:%email%">Email</a>\n#\n## Node style (placeholders are replaced once).\n## Default is the current style for nodes.\n#\n# style: label;image=%image%;whiteSpace=wrap;html=1;rounded=1;fillColor=%fill%;strokeColor=%stroke%;\n#\n## Parent style for nodes with child nodes (placeholders are replaced once).\n#\n# parentstyle: swimlane;whiteSpace=wrap;html=1;childLayout=stackLayout;horizontal=1;horizontalStack=0;resizeParent=1;resizeLast=0;collapsible=1;\n#\n## Optional column name that contains a reference to a named style in styles.\n## Default is the current style for nodes.\n#\n# stylename: -\n#\n## JSON for named styles of the form {"name": "style", "name": "style"} where style is a cell style with\n## placeholders that are replaced once.\n#\n# styles: -\n#\n## Optional column name that contains a reference to a named label in labels.\n## Default is the current label.\n#\n# labelname: -\n#\n## JSON for named labels of the form {"name": "label", "name": "label"} where label is a cell label with\n## placeholders.\n#\n# labels: -\n#\n## Uses the given column name as the identity for cells (updates existing cells).\n## Default is no identity (empty value or -).\n#\n# identity: -\n#\n## Uses the given column name as the parent reference for cells. Default is no parent (empty or -).\n## The identity above is used for resolving the reference so it must be specified.\n#\n# parent: -\n#\n## Adds a prefix to the identity of cells to make sure they do not collide with existing cells (whose\n## IDs are numbers from 0..n, sometimes with a GUID prefix in the context of realtime collaboration).\n## Default is csvimport-.\n#\n# namespace: csvimport-\n#\n## Connections between rows ("from": source colum, "to": target column).\n## Label, style and invert are optional. Defaults are \'\', current style and false.\n## If placeholders are used in the style, they are replaced with data from the source.\n## An optional placeholders can be set to target to use data from the target instead.\n## In addition to label, an optional fromlabel and tolabel can be used to name the column\n## that contains the text for the label in the edges source or target (invert ignored).\n## The label is concatenated in the form fromlabel + label + tolabel if all are defined.\n## Additional labels can be added by using an optional labels array with entries of the\n## form {"label": string, "x": number, "y": number, "dx": number, "dy": number} where\n## x is from -1 to 1 along the edge, y is orthogonal, and dx/dy are offsets in pixels.\n## The target column may contain a comma-separated list of values.\n## Multiple connect entries are allowed.\n#\n# connect: {"from": "manager", "to": "name", "invert": true, "label": "manages", \\\n#          "style": "curved=1;endArrow=blockThin;endFill=1;fontSize=11;"}\n# connect: {"from": "refs", "to": "id", "style": "curved=1;fontSize=11;"}\n#\n## Node x-coordinate. Possible value is a column name. Default is empty. Layouts will\n## override this value.\n#\n# left: \n#\n## Node y-coordinate. Possible value is a column name. Default is empty. Layouts will\n## override this value.\n#\n# top: \n#\n## Node width. Possible value is a number (in px), auto or an @ sign followed by a column\n## name that contains the value for the width. Default is auto.\n#\n# width: auto\n#\n## Node height. Possible value is a number (in px), auto or an @ sign followed by a column\n## name that contains the value for the height. Default is auto.\n#\n# height: auto\n#\n## Padding for autosize. Default is 0.\n#\n# padding: -12\n#\n## Comma-separated list of ignored columns for metadata. (These can be\n## used for connections and styles but will not be added as metadata.)\n#\n# ignore: id,image,fill,stroke,refs,manager\n#\n## Column to be renamed to link attribute (used as link).\n#\n# link: url\n#\n## Spacing between nodes. Default is 40.\n#\n# nodespacing: 40\n#\n## Spacing between levels of hierarchical layouts. Default is 100.\n#\n# levelspacing: 100\n#\n## Spacing between parallel edges. Default is 40. Use 0 to disable.\n#\n# edgespacing: 40\n#\n## Name or JSON of layout. Possible values are auto, none, verticaltree, horizontaltree,\n## verticalflow, horizontalflow, organic, circle or a JSON string as used in Layout, Apply.\n## Default is auto.\n#\n# layout: auto\n#\n## ---- CSV below this line. First line are column names. ----\nname,position,id,location,manager,email,fill,stroke,refs,url,image\nEvan Miller,CFO,emi,Office 1,,me@example.com,#dae8fc,#6c8ebf,,https://www.draw.io,https://cdn3.iconfinder.com/data/icons/user-avatars-1/512/users-9-2-128.png\nEdward Morrison,Brand Manager,emo,Office 2,Evan Miller,me@example.com,#d5e8d4,#82b366,,https://www.draw.io,https://cdn3.iconfinder.com/data/icons/user-avatars-1/512/users-10-3-128.png\nRon Donovan,System Admin,rdo,Office 3,Evan Miller,me@example.com,#d5e8d4,#82b366,"emo,tva",https://www.draw.io,https://cdn3.iconfinder.com/data/icons/user-avatars-1/512/users-2-128.png\nTessa Valet,HR Director,tva,Office 4,Evan Miller,me@example.com,#d5e8d4,#82b366,,https://www.draw.io,https://cdn3.iconfinder.com/data/icons/user-avatars-1/512/users-3-128.png\n';
    Editor.createRoughCanvas = function(a) {
        var d = rough.canvas({
            getContext: function() {
                return a
            }
        });
        d.draw = function(d) {
            var b = d.sets || [];
            d = d.options || this.getDefaultOptions();
            for (var c = 0; c < b.length; c++) {
                var e = b[c];
                switch (e.type) {
                    case "path":
                        null != d.stroke && this._drawToContext(a, e, d);
                        break;
                    case "fillPath":
                        this._drawToContext(a, e, d);
                        break;
                    case "fillSketch":
                        this.fillSketch(a, e, d)
                }
            }
        };
        d.fillSketch = function(d, b, c) {
            var e = a.state.strokeColor,
                g = a.state.strokeWidth,
                m = a.state.strokeAlpha,
                f = a.state.dashed,
                n = c.fillWeight;
            0 > n && (n = c.strokeWidth / 2);
            a.setStrokeAlpha(a.state.fillAlpha);
            a.setStrokeColor(c.fill || "");
            a.setStrokeWidth(n);
            a.setDashed(!1);
            this._drawToContext(d, b, c);
            a.setDashed(f);
            a.setStrokeWidth(g);
            a.setStrokeColor(e);
            a.setStrokeAlpha(m)
        };
        d._drawToContext = function(a, d, b) {
            a.begin();
            for (var c = 0; c < d.ops.length; c++) {
                var e = d.ops[c],
                    g = e.data;
                switch (e.op) {
                    case "move":
                        a.moveTo(g[0], g[1]);
                        break;
                    case "bcurveTo":
                        a.curveTo(g[0], g[1], g[2], g[3], g[4], g[5]);
                        break;
                    case "lineTo":
                        a.lineTo(g[0], g[1])
                }
            }
            a.end();
            "fillPath" === d.type && b.filled ? a.fill() : a.stroke()
        };
        return d
    };
    (function() {
        function a(d, b, c) {
            this.canvas = d;
            this.rc = b;
            this.shape = c;
            this.canvas.setLineJoin("round");
            this.canvas.setLineCap("round");
            this.originalBegin = this.canvas.begin;
            this.canvas.begin = mxUtils.bind(this, a.prototype.begin);
            this.originalEnd = this.canvas.end;
            this.canvas.end = mxUtils.bind(this, a.prototype.end);
            this.originalRect = this.canvas.rect;
            this.canvas.rect = mxUtils.bind(this, a.prototype.rect);
            this.originalRoundrect = this.canvas.roundrect;
            this.canvas.roundrect = mxUtils.bind(this, a.prototype.roundrect);
            this.originalEllipse = this.canvas.ellipse;
            this.canvas.ellipse = mxUtils.bind(this, a.prototype.ellipse);
            this.originalLineTo = this.canvas.lineTo;
            this.canvas.lineTo = mxUtils.bind(this, a.prototype.lineTo);
            this.originalMoveTo = this.canvas.moveTo;
            this.canvas.moveTo = mxUtils.bind(this, a.prototype.moveTo);
            this.originalQuadTo = this.canvas.quadTo;
            this.canvas.quadTo = mxUtils.bind(this, a.prototype.quadTo);
            this.originalCurveTo = this.canvas.curveTo;
            this.canvas.curveTo = mxUtils.bind(this, a.prototype.curveTo);
            this.originalArcTo = this.canvas.arcTo;
            this.canvas.arcTo = mxUtils.bind(this, a.prototype.arcTo);
            this.originalClose = this.canvas.close;
            this.canvas.close = mxUtils.bind(this, a.prototype.close);
            this.originalFill = this.canvas.fill;
            this.canvas.fill = mxUtils.bind(this, a.prototype.fill);
            this.originalStroke = this.canvas.stroke;
            this.canvas.stroke = mxUtils.bind(this, a.prototype.stroke);
            this.originalFillAndStroke = this.canvas.fillAndStroke;
            this.canvas.fillAndStroke = mxUtils.bind(this, a.prototype.fillAndStroke);
            this.path = [];
            this.passThrough = !1
        }
        a.prototype.moveOp = "M";
        a.prototype.lineOp = "L";
        a.prototype.quadOp = "Q";
        a.prototype.curveOp = "C";
        a.prototype.closeOp = "Z";
        a.prototype.getStyle = function(a, d) {
            var b = 1;
            if (null != this.shape.state) {
                var c = this.shape.state.cell.id;
                if (null != c)
                    for (var e = 0; e < c.length; e++) b = (b << 5) - b + c.charCodeAt(e) << 0
            }
            b = {
                strokeWidth: this.canvas.state.strokeWidth,
                seed: b
            };
            c = this.rc.getDefaultOptions();
            a ? b.stroke = "none" === this.canvas.state.strokeColor ? "transparent" : this.canvas.state.strokeColor : delete b.stroke;
            e = null;
            (b.filled = d) ? (b.fill = "none" === this.canvas.state.fillColor ? "" : this.canvas.state.fillColor, e = "none" === this.canvas.state.gradientColor ? null : this.canvas.state.gradientColor) : "" == b.fill;
            b.bowing = mxUtils.getValue(this.shape.style, "bowing", c.bowing);
            b.hachureAngle = mxUtils.getValue(this.shape.style, "hachureAngle", c.hachureAngle);
            b.curveFitting = mxUtils.getValue(this.shape.style, "curveFitting", c.curveFitting);
            b.roughness = mxUtils.getValue(this.shape.style, "jiggle", c.roughness);
            b.simplification = mxUtils.getValue(this.shape.style, "simplification", c.simplification);
            b.disableMultiStroke = mxUtils.getValue(this.shape.style, "disableMultiStroke", c.disableMultiStroke);
            b.disableMultiStrokeFill = mxUtils.getValue(this.shape.style, "disableMultiStrokeFill", c.disableMultiStrokeFill);
            var g = mxUtils.getValue(this.shape.style, "hachureGap", -1);
            b.hachureGap = "auto" == g ? -1 : g;
            b.dashGap = mxUtils.getValue(this.shape.style, "dashGap", g);
            b.dashOffset = mxUtils.getValue(this.shape.style, "dashOffset", g);
            b.zigzagOffset = mxUtils.getValue(this.shape.style, "zigzagOffset", g);
            g = mxUtils.getValue(this.shape.style, "fillWeight", -1);
            b.fillWeight = "auto" == g ? -1 : g;
            g = mxUtils.getValue(this.shape.style, "fillStyle", "auto");
            "auto" == g && (g = null != this.shape.state ? this.shape.state.view.graph.defaultPageBackgroundColor : "#ffffff", g = null != b.fill && (null != e || null != g && b.fill.toLowerCase() == g.toLowerCase()) ? "solid" : c.fillStyle);
            b.fillStyle = g;
            return b
        };
        a.prototype.begin = function() {
            this.passThrough ? this.originalBegin.apply(this.canvas, arguments) : this.path = []
        };
        a.prototype.end = function() {
            this.passThrough && this.originalEnd.apply(this.canvas, arguments)
        };
        a.prototype.addOp = function() {
            if (null != this.path && (this.path.push(arguments[0]), 2 < arguments.length))
                for (var a = 2; a < arguments.length; a += 2) this.lastX = arguments[a - 1], this.lastY = arguments[a], this.path.push(this.canvas.format(this.lastX)), this.path.push(this.canvas.format(this.lastY))
        };
        a.prototype.lineTo = function(a, d) {
            this.passThrough ? this.originalLineTo.apply(this.canvas, arguments) : (this.addOp(this.lineOp, a, d), this.lastX = a, this.lastY = d)
        };
        a.prototype.moveTo = function(a, d) {
            this.passThrough ? this.originalMoveTo.apply(this.canvas, arguments) : (this.addOp(this.moveOp, a, d), this.lastX = a, this.lastY = d, this.firstX = a, this.firstY = d)
        };
        a.prototype.close = function() {
            this.passThrough ? this.originalClose.apply(this.canvas, arguments) : this.addOp(this.closeOp)
        };
        a.prototype.quadTo = function(a, d, b, c) {
            this.passThrough ? this.originalQuadTo.apply(this.canvas, arguments) : (this.addOp(this.quadOp, a, d, b, c), this.lastX = b, this.lastY = c)
        };
        a.prototype.curveTo = function(a, d, b, c, e, g) {
            this.passThrough ? this.originalCurveTo.apply(this.canvas, arguments) : (this.addOp(this.curveOp, a, d, b, c, e, g), this.lastX = e, this.lastY = g)
        };
        a.prototype.arcTo = function(a, d, b, c, e, g, m) {
            if (this.passThrough) this.originalArcTo.apply(this.canvas, arguments);
            else {
                var f = mxUtils.arcToCurves(this.lastX, this.lastY, a, d, b, c, e, g, m);
                if (null != f)
                    for (var n = 0; n < f.length; n += 6) this.curveTo(f[n], f[n + 1], f[n + 2], f[n + 3], f[n + 4], f[n + 5]);
                this.lastX = g;
                this.lastY = m
            }
        };
        a.prototype.rect = function(a, d, b, c) {
            this.passThrough ? this.originalRect.apply(this.canvas, arguments) : (this.path = [], this.nextShape = this.rc.generator.rectangle(a, d, b, c, this.getStyle(!0, !0)))
        };
        a.prototype.ellipse = function(a, d, b, c) {
            this.passThrough ? this.originalEllipse.apply(this.canvas, arguments) : (this.path = [], this.nextShape = this.rc.generator.ellipse(a + b / 2, d + c / 2, b, c, this.getStyle(!0, !0)))
        };
        a.prototype.roundrect = function(a, d, b, c, e, g) {
            this.passThrough ? this.originalRoundrect.apply(this.canvas, arguments) : (this.begin(), this.moveTo(a + e, d), this.lineTo(a + b - e, d), this.quadTo(a + b, d, a + b, d + g), this.lineTo(a + b, d + c - g), this.quadTo(a + b, d + c, a + b - e, d + c), this.lineTo(a + e, d + c), this.quadTo(a, d + c, a, d + c - g), this.lineTo(a, d + g), this.quadTo(a, d, a + e, d))
        };
        a.prototype.drawPath = function(a) {
            if (0 < this.path.length) {
                this.passThrough = !0;
                try {
                    this.rc.path(this.path.join(" "), a)
                } catch (J) {}
                this.passThrough = !1
            } else if (null != this.nextShape) {
                for (var d in a) this.nextShape.options[d] = a[d];
                null == a.stroke && delete this.nextShape.options.stroke;
                a.filled || delete this.nextShape.options.fill;
                this.passThrough = !0;
                this.rc.draw(this.nextShape);
                this.passThrough = !1
            }
        };
        a.prototype.stroke = function() {
            this.passThrough ? this.originalStroke.apply(this.canvas, arguments) : this.drawPath(this.getStyle(!0, !1))
        };
        a.prototype.fill = function() {
            this.passThrough ? this.originalFill.apply(this.canvas, arguments) : this.drawPath(this.getStyle(!1, !0))
        };
        a.prototype.fillAndStroke = function() {
            this.passThrough ? this.originalFillAndStroke.apply(this.canvas, arguments) : this.drawPath(this.getStyle(!0, !0))
        };
        a.prototype.destroy = function() {
            this.canvas.lineTo = this.originalLineTo;
            this.canvas.moveTo = this.originalMoveTo;
            this.canvas.close = this.originalClose;
            this.canvas.quadTo = this.originalQuadTo;
            this.canvas.curveTo = this.originalCurveTo;
            this.canvas.arcTo = this.originalArcTo;
            this.canvas.close = this.originalClose;
            this.canvas.fill = this.originalFill;
            this.canvas.stroke = this.originalStroke;
            this.canvas.fillAndStroke = this.originalFillAndStroke;
            this.canvas.begin = this.originalBegin;
            this.canvas.end = this.originalEnd;
            this.canvas.rect = this.originalRect;
            this.canvas.ellipse = this.originalEllipse;
            this.canvas.roundrect = this.originalRoundrect
        };
        mxShape.prototype.createRoughCanvas = function(d) {
            return new a(d, Editor.createRoughCanvas(d), this)
        };
        var d = mxShape.prototype.createHandJiggle;
        mxShape.prototype.createHandJiggle = function(a) {
            return this.outline || null == this.style || "0" == mxUtils.getValue(this.style, "sketch", "1" == urlParams.rough ? "1" : "0") ? d.apply(this, arguments) : "comic" == mxUtils.getValue(this.style, "sketchStyle", "rough") ? this.createComicCanvas(a) : this.createRoughCanvas(a)
        };
        var b = mxShape.prototype.paint;
        mxShape.prototype.paint = function(d) {
            var c = null,
                e = !0;
            if (null != this.style && (e = "1" == mxUtils.getValue(this.style, mxConstants.STYLE_POINTER_EVENTS, "1"), c = mxUtils.getValue(this.style, "fillStyle", "auto"), null != this.state && "auto" == c)) {
                var g = this.state.view.graph.defaultPageBackgroundColor;
                null != this.fill && (null != this.gradient || null != g && this.fill.toLowerCase() == g.toLowerCase()) && (c = "solid")
            }!e || null == d.handJiggle || d.handJiggle.constructor != a || this.outline || null != this.fill && this.fill != mxConstants.NONE && "solid" == c || (d.save(), c = this.fill, e = this.stroke, this.stroke = this.fill = null, d.handJiggle.passThrough = !0, b.apply(this, arguments), d.handJiggle.passThrough = !1, this.fill = c, this.stroke = e, d.restore());
            b.apply(this, arguments)
        }
    })();
    Editor.fastCompress = function(a) {
        return null == a || 0 == a.length || "undefined" === typeof pako ? a : pako.deflateRaw(a, {
            to: "string"
        })
    };
    Editor.fastDecompress = function(a) {
        return null == a || 0 == a.length || "undefined" === typeof pako ? a : pako.inflateRaw(a, {
            to: "string"
        })
    };
    Editor.extractGraphModel = function(a, d, b) {
        if (null != a && "undefined" !== typeof pako) {
            var c = a.ownerDocument.getElementsByTagName("div"),
                e = [];
            if (null != c && 0 < c.length)
                for (var g = 0; g < c.length; g++)
                    if ("mxgraph" == c[g].getAttribute("class")) {
                        e.push(c[g]);
                        break
                    }
            0 < e.length && (c = e[0].getAttribute("data-mxgraph"), null != c ? (e = JSON.parse(c), null != e && null != e.xml && (a = mxUtils.parseXml(e.xml), a = a.documentElement)) : (e = e[0].getElementsByTagName("div"), 0 < e.length && (c = mxUtils.getTextContent(e[0]), c = Graph.decompress(c, null, b), 0 < c.length && (a = mxUtils.parseXml(c), a = a.documentElement))))
        }
        if (null != a && "svg" == a.nodeName)
            if (c = a.getAttribute("content"), null != c && "<" != c.charAt(0) && "%" != c.charAt(0) && (c = unescape(window.atob ? atob(c) : Base64.decode(cont, c))), null != c && "%" == c.charAt(0) && (c = decodeURIComponent(c)), null != c && 0 < c.length) a = mxUtils.parseXml(c).documentElement;
            else throw {
                message: mxResources.get("notADiagramFile")
            };
        null == a || d || (e = null, "diagram" == a.nodeName ? e = a : "mxfile" == a.nodeName && (c = a.getElementsByTagName("diagram"), 0 < c.length && (e = c[Math.max(0, Math.min(c.length - 1, urlParams.page || 0))])), null != e && (a = Editor.parseDiagramNode(e, b)));
        null == a || "mxGraphModel" == a.nodeName || d && "mxfile" == a.nodeName || (a = null);
        return a
    };
    Editor.parseDiagramNode = function(a, d) {
        var b = mxUtils.trim(mxUtils.getTextContent(a)),
            c = null;
        0 < b.length ? (b = Graph.decompress(b, null, d), null != b && 0 < b.length && (c = mxUtils.parseXml(b).documentElement)) : (b = mxUtils.getChildNodes(a), 0 < b.length && (c = mxUtils.createXmlDocument(), c.appendChild(c.importNode(b[0], !0)), c = c.documentElement));
        return c
    };
    Editor.getDiagramNodeXml = function(a) {
        var d = mxUtils.getTextContent(a),
            b = null;
        0 < d.length ? b = Graph.decompress(d) : null != a.firstChild && (b = mxUtils.getXml(a.firstChild));
        return b
    };
    Editor.extractGraphModelFromPdf = function(a) {
        a = a.substring(a.indexOf(",") + 1);
        a = window.atob && !mxClient.IS_SF ? atob(a) : Base64.decode(a, !0);
        for (var d = null, b = "", c = 0, e = 0, g = [], m = null; e < a.length;) {
            var f = a.charCodeAt(e),
                e = e + 1;
            10 != f && (b += String.fromCharCode(f));
            f == "/Subject (%3Cmxfile".charCodeAt(c) ? c++ : c = 0;
            if (19 == c) {
                var n = a.indexOf("%3C%2Fmxfile%3E)", e) + 15,
                    e = e - 9;
                if (n > e) {
                    d = a.substring(e, n);
                    break
                }
            }
            10 == f && ("endobj" == b ? m = null : "obj" == b.substring(b.length - 3, b.length) || "xref" == b || "trailer" == b ? (m = [], g[b.split(" ")[0]] = m) : null != m && m.push(b), b = "")
        }
        null == d && (d = Editor.extractGraphModelFromXref(g));
        null != d && (d = decodeURIComponent(d.replace(/\\\(/g, "(").replace(/\\\)/g, ")")));
        return d
    };
    Editor.extractGraphModelFromXref = function(a) {
        var d = a.trailer,
            b = null;
        null != d && (d = /.* \/Info (\d+) (\d+) R/g.exec(d.join("\n")), null != d && 0 < d.length && (d = a[d[1]], null != d && (d = /.* \/Subject (\d+) (\d+) R/g.exec(d.join("\n")), null != d && 0 < d.length && (a = a[d[1]], null != a && (a = a.join("\n"), b = a.substring(1, a.length - 1))))));
        return b
    };
    Editor.extractGraphModelFromPng = function(a) {
        var d = null;
        try {
            var b = a.substring(a.indexOf(",") + 1),
                c = window.atob && !mxClient.IS_SF ? atob(b) : Base64.decode(b, !0);
            EditorUi.parsePng(c, mxUtils.bind(this, function(a, b, e) {
                a = c.substring(a + 8, a + 8 + e);
                "zTXt" == b ? (e = a.indexOf(String.fromCharCode(0)), "mxGraphModel" == a.substring(0, e) && (a = pako.inflateRaw(a.substring(e + 2), {
                    to: "string"
                }).replace(/\+/g, " "), null != a && 0 < a.length && (d = a))) : "tEXt" == b && (a = a.split(String.fromCharCode(0)), 1 < a.length && ("mxGraphModel" == a[0] || "mxfile" == a[0]) && (d = a[1]));
                if (null != d || "IDAT" == b) return !0
            }))
        } catch (O) {}
        null != d && "%" == d.charAt(0) && (d = decodeURIComponent(d));
        null != d && "%" == d.charAt(0) && (d = decodeURIComponent(d));
        return d
    };
    Editor.extractParserError = function(a, d) {
        var b = null,
            c = null != a ? a.getElementsByTagName("parsererror") : null;
        null != c && 0 < c.length && (b = d || mxResources.get("invalidChars"), c = c[0].getElementsByTagName("div"), 0 < c.length && (b = mxUtils.getTextContent(c[0])));
        return null != b ? mxUtils.trim(b) : b
    };
    Editor.addRetryToError = function(a, d) {
        if (null != a) {
            var b = null != a.error ? a.error : a;
            null == b.retry && (b.retry = d)
        }
    };
    Editor.configure = function(a, d) {
        if (null != a) {
            Editor.config = a;
            Editor.configVersion = a.version;
            Menus.prototype.defaultFonts = a.defaultFonts || Menus.prototype.defaultFonts;
            ColorDialog.prototype.presetColors = a.presetColors || ColorDialog.prototype.presetColors;
            ColorDialog.prototype.defaultColors = a.defaultColors || ColorDialog.prototype.defaultColors;
            StyleFormatPanel.prototype.defaultColorSchemes = a.defaultColorSchemes || StyleFormatPanel.prototype.defaultColorSchemes;
            Graph.prototype.defaultEdgeLength = a.defaultEdgeLength || Graph.prototype.defaultEdgeLength;
            DrawioFile.prototype.autosaveDelay = a.autosaveDelay || DrawioFile.prototype.autosaveDelay;
            null != a.templateFile && (EditorUi.templateFile = a.templateFile);
            null != a.styles && (Editor.styles = a.styles);
            null != a.globalVars && (Editor.globalVars = a.globalVars);
            null != a.compressXml && (Editor.compressXml = a.compressXml);
            a.customFonts && (Menus.prototype.defaultFonts = a.customFonts.concat(Menus.prototype.defaultFonts));
            a.customPresetColors && (ColorDialog.prototype.presetColors = a.customPresetColors.concat(ColorDialog.prototype.presetColors));
            null != a.customColorSchemes && (StyleFormatPanel.prototype.defaultColorSchemes = a.customColorSchemes.concat(StyleFormatPanel.prototype.defaultColorSchemes));
            if (null != a.css) {
                var b = document.createElement("style");
                b.setAttribute("type", "text/css");
                b.appendChild(document.createTextNode(a.css));
                var c = document.getElementsByTagName("script")[0];
                c.parentNode.insertBefore(b, c)
            }
            null != a.libraries && (Sidebar.prototype.customEntries = a.libraries);
            null != a.enabledLibraries && (Sidebar.prototype.enabledLibraries = a.enabledLibraries);
            null != a.defaultLibraries && (Sidebar.prototype.defaultEntries = a.defaultLibraries);
            null != a.defaultCustomLibraries && (Editor.defaultCustomLibraries = a.defaultCustomLibraries);
            null != a.enableCustomLibraries && (Editor.enableCustomLibraries = a.enableCustomLibraries);
            null != a.defaultVertexStyle && (Graph.prototype.defaultVertexStyle = a.defaultVertexStyle);
            null != a.defaultEdgeStyle && (Graph.prototype.defaultEdgeStyle = a.defaultEdgeStyle);
            a.emptyDiagramXml && (EditorUi.prototype.emptyDiagramXml = a.emptyDiagramXml);
            a.thumbWidth && (Sidebar.prototype.thumbWidth = a.thumbWidth);
            a.thumbHeight && (Sidebar.prototype.thumbHeight = a.thumbHeight);
            a.emptyLibraryXml && (EditorUi.prototype.emptyLibraryXml = a.emptyLibraryXml);
            a.sidebarWidth && (EditorUi.prototype.hsplitPosition = a.sidebarWidth);
            a.fontCss && Editor.configureFontCss(a.fontCss);
            null != a.autosaveDelay && (b = parseInt(a.autosaveDelay), !isNaN(b) && 0 < b ? DrawioFile.prototype.autosaveDelay = b : EditorUi.debug("Invalid autosaveDelay: " + a.autosaveDelay));
            if (null != a.plugins && !d)
                for (App.initPluginCallback(), b = 0; b < a.plugins.length; b++) mxscript(a.plugins[b]);
            null != a.maxImageBytes && (EditorUi.prototype.maxImageBytes = a.maxImageBytes);
            null != a.maxImageSize && (EditorUi.prototype.maxImageSize = a.maxImageSize)
        }
    };
    Editor.configureFontCss = function(a) {
        if (null != a) {
            Editor.prototype.fontCss = a;
            var d = document.getElementsByTagName("script")[0];
            if (null != d && null != d.parentNode) {
                var b = document.createElement("style");
                b.setAttribute("type", "text/css");
                b.appendChild(document.createTextNode(a));
                d.parentNode.insertBefore(b, d);
                a = a.split("url(");
                for (b = 1; b < a.length; b++) {
                    var c = a[b].indexOf(")"),
                        c = Editor.trimCssUrl(a[b].substring(0, c)),
                        e = document.createElement("link");
                    e.setAttribute("rel", "preload");
                    e.setAttribute("href", c);
                    e.setAttribute("as", "font");
                    e.setAttribute("crossorigin", "");
                    d.parentNode.insertBefore(e, d)
                }
            }
        }
    };
    Editor.trimCssUrl = function(a) {
        return a.replace(RegExp("^[\\s\"']+", "g"), "").replace(RegExp("[\\s\"']+$", "g"), "")
    };
    Editor.GOOGLE_FONTS = "https://fonts.googleapis.com/css?family=";
    Editor.GUID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";
    Editor.GUID_LENGTH = 20;
    Editor.guid = function(a) {
        a = null != a ? a : Editor.GUID_LENGTH;
        for (var d = [], b = 0; b < a; b++) d.push(Editor.GUID_ALPHABET.charAt(Math.floor(Math.random() * Editor.GUID_ALPHABET.length)));
        return d.join("")
    };
    Editor.prototype.timeout = 25E3;
    Editor.prototype.useForeignObjectForMath = !mxClient.IS_SF;
    Editor.prototype.editButtonLink = null != urlParams.edit ? decodeURIComponent(urlParams.edit) : null;
    Editor.prototype.crossOriginImages = !mxClient.IS_IE;
    var a = Editor.prototype.setGraphXml;
    Editor.prototype.setGraphXml = function(d) {
        d = null != d && "mxlibrary" != d.nodeName ? this.extractGraphModel(d) : null;
        if (null != d) {
            var b = d.getElementsByTagName("parsererror");
            if (null != b && 0 < b.length) {
                var b = b[0],
                    c = b.getElementsByTagName("div");
                null != c && 0 < c.length && (b = c[0]);
                throw {
                    message: mxUtils.getTextContent(b)
                };
            }
            if ("mxGraphModel" == d.nodeName) {
                b = d.getAttribute("style") || "default-style2";
                if ("1" == urlParams.embed || null != b && "" != b) b != this.graph.currentStyle && (c = null != this.graph.themes ? this.graph.themes[b] : mxUtils.load(STYLE_PATH + "/" + b + ".xml").getDocumentElement(), null != c && (e = new mxCodec(c.ownerDocument), e.decode(c, this.graph.getStylesheet())));
                else if (c = null != this.graph.themes ? this.graph.themes["default-old"] : mxUtils.load(STYLE_PATH + "/default-old.xml").getDocumentElement(), null != c) {
                    var e = new mxCodec(c.ownerDocument);
                    e.decode(c, this.graph.getStylesheet())
                }
                this.graph.currentStyle = b;
                this.graph.mathEnabled = "1" == urlParams.math || "1" == d.getAttribute("math");
                b = d.getAttribute("backgroundImage");
                null != b ? (b = JSON.parse(b), this.graph.setBackgroundImage(new mxImage(b.src, b.width, b.height))) : this.graph.setBackgroundImage(null);
                mxClient.NO_FO = this.graph.mathEnabled && !this.useForeignObjectForMath ? !0 : this.originalNoForeignObject;
                this.graph.useCssTransforms = !mxClient.NO_FO && this.isChromelessView() && this.graph.isCssTransformsSupported();
                this.graph.updateCssTransform();
                this.graph.setShadowVisible("1" == d.getAttribute("shadow"), !1);
                if (b = d.getAttribute("extFonts")) try {
                    for (b = b.split("|").map(function(a) {
                            a = a.split("^");
                            return {
                                name: a[0],
                                url: a[1]
                            }
                        }), c = 0; c < b.length; c++) this.graph.addExtFont(b[c].name, b[c].url)
                } catch (O) {
                    console.log("ExtFonts format error: " + O.message)
                }
            }
            a.apply(this, arguments)
        } else throw {
            message: mxResources.get("notADiagramFile") || "Invalid data",
            toString: function() {
                return this.message
            }
        };
    };
    var e = Editor.prototype.getGraphXml;
    Editor.prototype.getGraphXml = function(a) {
        a = null != a ? a : !0;
        var d = e.apply(this, arguments);
        null != this.graph.currentStyle && "default-style2" != this.graph.currentStyle && d.setAttribute("style", this.graph.currentStyle);
        null != this.graph.backgroundImage && d.setAttribute("backgroundImage", JSON.stringify(this.graph.backgroundImage));
        d.setAttribute("math", this.graph.mathEnabled ? "1" : "0");
        d.setAttribute("shadow", this.graph.shadowVisible ? "1" : "0");
        if (null != this.graph.extFonts && 0 < this.graph.extFonts.length) {
            var b = this.graph.extFonts.map(function(a) {
                return a.name + "^" + a.url
            });
            d.setAttribute("extFonts", b.join("|"))
        }
        return d
    };
    Editor.prototype.isDataSvg = function(a) {
        try {
            var d = mxUtils.parseXml(a).documentElement.getAttribute("content");
            if (null != d && (null != d && "<" != d.charAt(0) && "%" != d.charAt(0) && (d = unescape(window.atob ? atob(d) : Base64.decode(cont, d))), null != d && "%" == d.charAt(0) && (d = decodeURIComponent(d)), null != d && 0 < d.length)) {
                var b = mxUtils.parseXml(d).documentElement;
                return "mxfile" == b.nodeName || "mxGraphModel" == b.nodeName
            }
        } catch (L) {}
        return !1
    };
    Editor.prototype.extractGraphModel = function(a, d, b) {
        return Editor.extractGraphModel.apply(this, arguments)
    };
    var c = Editor.prototype.resetGraph;
    Editor.prototype.resetGraph = function() {
        this.graph.mathEnabled = "1" == urlParams.math;
        this.graph.view.x0 = null;
        this.graph.view.y0 = null;
        mxClient.NO_FO = this.graph.mathEnabled && !this.useForeignObjectForMath ? !0 : this.originalNoForeignObject;
        this.graph.useCssTransforms = !mxClient.NO_FO && this.isChromelessView() && this.graph.isCssTransformsSupported();
        this.graph.updateCssTransform();
        c.apply(this, arguments)
    };
    var b = Editor.prototype.updateGraphComponents;
    Editor.prototype.updateGraphComponents = function() {
        b.apply(this, arguments);
        mxClient.NO_FO = this.graph.mathEnabled && !this.useForeignObjectForMath && null != Editor.MathJaxRender ? !0 : this.originalNoForeignObject;
        this.graph.useCssTransforms = !mxClient.NO_FO && this.isChromelessView() && this.graph.isCssTransformsSupported();
        this.graph.updateCssTransform()
    };
    Editor.initMath = function(a, d) {
        a = null != a ? a : DRAW_MATH_URL + "/MathJax.js";
        Editor.mathJaxQueue = [];
        Editor.doMathJaxRender = function(a) {
            window.setTimeout(function() {
                "hidden" != a.style.visibility && MathJax.Hub.Queue(["Typeset",
                    MathJax.Hub, a
                ])
            }, 0)
        };
        var b = null != urlParams["math-font"] ? decodeURIComponent(urlParams["math-font"]) : "TeX";
        d = null != d ? d : {
            jax: ["input/TeX", "input/MathML", "input/AsciiMath"].concat(["html" == urlParams["math-output"] ? "output/HTML-CSS" : "output/SVG"]),
            extensions: ["tex2jax.js", "mml2jax.js", "asciimath2jax.js"],
            TeX: {
                extensions: ["AMSmath.js", "AMSsymbols.js", "noErrors.js", "noUndefined.js"]
            },
            "HTML-CSS": {
                availableFonts: [b],
                imageFont: null
            },
            SVG: {
                font: b,
                useFontCache: !1
            },
            tex2jax: {
                ignoreClass: "mxCellEditor"
            },
            asciimath2jax: {
                ignoreClass: "mxCellEditor"
            }
        };
        window.MathJax = {
            skipStartupTypeset: !0,
            showMathMenu: !1,
            messageStyle: "none",
            AuthorInit: function() {
                MathJax.Hub.Config(d);
                MathJax.Hub.Register.StartupHook("Begin", function() {
                    for (var a = 0; a < Editor.mathJaxQueue.length; a++) Editor.doMathJaxRender(Editor.mathJaxQueue[a])
                })
            }
        };
        Editor.MathJaxRender = function(a) {
            "undefined" !== typeof MathJax && "undefined" !== typeof MathJax.Hub ? Editor.doMathJaxRender(a) : Editor.mathJaxQueue.push(a)
        };
        Editor.MathJaxClear = function() {
            Editor.mathJaxQueue = []
        };
        var c = Editor.prototype.init;
        Editor.prototype.init = function() {
            c.apply(this, arguments);
            this.graph.addListener(mxEvent.SIZE, mxUtils.bind(this, function(a, d) {
                null != this.graph.container && this.graph.mathEnabled && !this.graph.blockMathRender && Editor.MathJaxRender(this.graph.container)
            }))
        };
        b = document.getElementsByTagName("script");
        if (null != b && 0 < b.length) {
            var e = document.createElement("script");
            e.type = "text/javascript";
            e.src = a;
            b[0].parentNode.appendChild(e)
        }
    };
    Editor.prototype.csvToArray = function(a) {
        if (!/^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/.test(a)) return null;
        var d = [];
        a.replace(/(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g, function(a, b, c, e) {
            void 0 !== b ? d.push(b.replace(/\\'/g, "'")) : void 0 !== c ? d.push(c.replace(/\\"/g, '"')) : void 0 !== e && d.push(e);
            return ""
        });
        /,\s*$/.test(a) && d.push("");
        return d
    };
    Editor.prototype.isCorsEnabledForUrl = function(a) {
        if (mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) return !0;
        null != urlParams.cors && null == this.corsRegExp && (this.corsRegExp = new RegExp(decodeURIComponent(urlParams.cors)));
        return null != this.corsRegExp && this.corsRegExp.test(a) || "https://raw.githubusercontent.com/" === a.substring(0, 34)
    };
    Editor.prototype.createImageUrlConverter = function() {
        var a = new mxUrlConverter;
        a.updateBaseUrl();
        var d = a.convert,
            b = this;
        a.convert = function(c) {
            if (null != c) {
                var e = "http://" == c.substring(0, 7) || "https://" == c.substring(0, 8);
                e && !navigator.onLine ? c = Editor.svgBrokenImage.src : !e || c.substring(0, a.baseUrl.length) == a.baseUrl || b.crossOriginImages && b.isCorsEnabledForUrl(c) ? "chrome-extension://" == c.substring(0, 19) || mxClient.IS_CHROMEAPP || (c = d.apply(this, arguments)) : c = PROXY_URL + "?url=" + encodeURIComponent(c)
            }
            return c
        };
        return a
    };
    Editor.createSvgDataUri = function(a) {
        return "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(a)))
    };
    Editor.prototype.convertImageToDataUri = function(a, d) {
        try {
            var b = !0,
                c = window.setTimeout(mxUtils.bind(this, function() {
                    b = !1;
                    d(Editor.svgBrokenImage.src)
                }), this.timeout);
            if (/(\.svg)$/i.test(a)) mxUtils.get(a, mxUtils.bind(this, function(a) {
                window.clearTimeout(c);
                b && d(Editor.createSvgDataUri(a.getText()))
            }), function() {
                window.clearTimeout(c);
                b && d(Editor.svgBrokenImage.src)
            });
            else {
                var e = new Image;
                this.crossOriginImages && (e.crossOrigin = "anonymous");
                e.onload = function() {
                    window.clearTimeout(c);
                    if (b) try {
                        var a = document.createElement("canvas"),
                            g = a.getContext("2d");
                        a.height = e.height;
                        a.width = e.width;
                        g.drawImage(e, 0, 0);
                        d(a.toDataURL())
                    } catch (W) {
                        d(Editor.svgBrokenImage.src)
                    }
                };
                e.onerror = function() {
                    window.clearTimeout(c);
                    b && d(Editor.svgBrokenImage.src)
                };
                e.src = a
            }
        } catch (J) {
            d(Editor.svgBrokenImage.src)
        }
    };
    Editor.prototype.convertImages = function(a, d, b, c) {
        null == c && (c = this.createImageUrlConverter());
        var e = 0,
            g = b || {};
        b = mxUtils.bind(this, function(b, m) {
            for (var f = a.getElementsByTagName(b), n = 0; n < f.length; n++) mxUtils.bind(this, function(b) {
                try {
                    if (null != b) {
                        var f = c.convert(b.getAttribute(m));
                        if (null != f && "data:" != f.substring(0, 5)) {
                            var n = g[f];
                            null == n ? (e++, this.convertImageToDataUri(f, function(c) {
                                null != c && (g[f] = c, b.setAttribute(m, c));
                                e--;
                                0 == e && d(a)
                            })) : b.setAttribute(m, n)
                        } else null != f && b.setAttribute(m, f)
                    }
                } catch (Z) {}
            })(f[n])
        });
        b("image", "xlink:href");
        b("img", "src");
        0 == e && d(a)
    };
    Editor.base64Encode = function(a) {
        for (var d = "", b = 0, c = a.length, e, g, m; b < c;) {
            e = a.charCodeAt(b++) & 255;
            if (b == c) {
                d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt(e >> 2);
                d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt((e & 3) << 4);
                d += "==";
                break
            }
            g = a.charCodeAt(b++);
            if (b == c) {
                d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt(e >> 2);
                d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt((e & 3) << 4 | (g & 240) >> 4);
                d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt((g & 15) << 2);
                d += "=";
                break
            }
            m = a.charCodeAt(b++);
            d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt(e >> 2);
            d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt((e & 3) << 4 | (g & 240) >> 4);
            d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt((g & 15) << 2 | (m & 192) >> 6);
            d += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".charAt(m & 63)
        }
        return d
    };
    Editor.prototype.loadUrl = function(a, d, b, c, e, g, m, f) {
        try {
            var n = !m && (c || /(\.png)($|\?)/i.test(a) || /(\.jpe?g)($|\?)/i.test(a) || /(\.gif)($|\?)/i.test(a) || /(\.pdf)($|\?)/i.test(a));
            e = null != e ? e : !0;
            var p = mxUtils.bind(this, function() {
                mxUtils.get(a, mxUtils.bind(this, function(a) {
                    if (200 <= a.getStatus() && 299 >= a.getStatus()) {
                        if (null != d) {
                            var c = a.getText();
                            if (n) {
                                if ((9 == document.documentMode || 10 == document.documentMode) && "undefined" !== typeof window.mxUtilsBinaryToArray) {
                                    a = mxUtilsBinaryToArray(a.request.responseBody).toArray();
                                    for (var c = Array(a.length), e = 0; e < a.length; e++) c[e] = String.fromCharCode(a[e]);
                                    c = c.join("")
                                }
                                g = null != g ? g : "data:image/png;base64,";
                                c = g + Editor.base64Encode(c)
                            }
                            d(c)
                        }
                    } else null != b && (0 == a.getStatus() ? b({
                        message: mxResources.get("accessDenied")
                    }, a) : b({
                        message: mxResources.get("error") + " " + a.getStatus()
                    }, a))
                }), function(a) {
                    null != b && b({
                        message: mxResources.get("error") + " " + a.getStatus()
                    })
                }, n, this.timeout, function() {
                    e && null != b && b({
                        code: App.ERROR_TIMEOUT,
                        retry: p
                    })
                }, f)
            });
            p()
        } catch (Y) {
            null != b && b(Y)
        }
    };
    Editor.prototype.absoluteCssFonts = function(a) {
        var d = null;
        if (null != a) {
            var b = a.split("url(");
            if (0 < b.length) {
                d = [b[0]];
                a = window.location.pathname;
                var c = null != a ? a.lastIndexOf("/") : -1;
                0 <= c && (a = a.substring(0, c + 1));
                var c = document.getElementsByTagName("base"),
                    e = null;
                null != c && 0 < c.length && (e = c[0].getAttribute("href"));
                for (var g = 1; g < b.length; g++)
                    if (c = b[g].indexOf(")"), 0 < c) {
                        var m = Editor.trimCssUrl(b[g].substring(0, c));
                        this.graph.isRelativeUrl(m) && (m = null != e ? e + m : window.location.protocol + "//" + window.location.hostname + ("/" == m.charAt(0) ? "" : a) + m);
                        d.push('url("' + m + '"' + b[g].substring(c))
                    } else d.push(b[g])
            } else d = [a]
        }
        return null != d ? d.join("") : null
    };
    Editor.prototype.embedCssFonts = function(a, d) {
        var b = a.split("url("),
            c = 0;
        null == this.cachedFonts && (this.cachedFonts = {});
        var e = mxUtils.bind(this, function() {
            if (0 == c) {
                for (var a = [b[0]], e = 1; e < b.length; e++) {
                    var g = b[e].indexOf(")");
                    a.push('url("');
                    a.push(this.cachedFonts[Editor.trimCssUrl(b[e].substring(0, g))]);
                    a.push('"' + b[e].substring(g))
                }
                d(a.join(""))
            }
        });
        if (0 < b.length) {
            for (var g = 1; g < b.length; g++) {
                var m = b[g].indexOf(")"),
                    f = null,
                    n = b[g].indexOf("format(", m);
                0 < n && (f = Editor.trimCssUrl(b[g].substring(n + 7, b[g].indexOf(")", n))));
                mxUtils.bind(this, function(a) {
                    if (null == this.cachedFonts[a]) {
                        this.cachedFonts[a] = a;
                        c++;
                        var d = "application/x-font-ttf";
                        if ("svg" == f || /(\.svg)($|\?)/i.test(a)) d = "image/svg+xml";
                        else if ("otf" == f || "embedded-opentype" == f || /(\.otf)($|\?)/i.test(a)) d = "application/x-font-opentype";
                        else if ("woff" == f || /(\.woff)($|\?)/i.test(a)) d = "application/font-woff";
                        else if ("woff2" == f || /(\.woff2)($|\?)/i.test(a)) d = "application/font-woff2";
                        else if ("eot" == f || /(\.eot)($|\?)/i.test(a)) d = "application/vnd.ms-fontobject";
                        else if ("sfnt" == f || /(\.sfnt)($|\?)/i.test(a)) d = "application/font-sfnt";
                        var b = a;
                        /^https?:\/\//.test(b) && !this.isCorsEnabledForUrl(b) && (b = PROXY_URL + "?url=" + encodeURIComponent(a));
                        this.loadUrl(b, mxUtils.bind(this, function(d) {
                            this.cachedFonts[a] = d;
                            c--;
                            e()
                        }), mxUtils.bind(this, function(a) {
                            c--;
                            e()
                        }), !0, null, "data:" + d + ";charset=utf-8;base64,")
                    }
                })(Editor.trimCssUrl(b[g].substring(0, m)), f)
            }
            e()
        } else d(a)
    };
    Editor.prototype.loadFonts = function(a) {
        null != this.fontCss && null == this.resolvedFontCss ? this.embedCssFonts(this.fontCss, mxUtils.bind(this, function(d) {
            this.resolvedFontCss = d;
            a()
        })) : a()
    };
    Editor.prototype.embedExtFonts = function(a) {
        var d = this.graph.extFonts;
        if (null != d && 0 < d.length) {
            var b = "",
                c = 0;
            null == this.cachedGoogleFonts && (this.cachedGoogleFonts = {});
            for (var e = mxUtils.bind(this, function() {
                    0 == c && this.embedCssFonts(b, a)
                }), g = 0; g < d.length; g++) mxUtils.bind(this, function(a, d) {
                0 == d.indexOf(Editor.GOOGLE_FONTS) ? null == this.cachedGoogleFonts[d] ? (c++, this.loadUrl(d, mxUtils.bind(this, function(a) {
                    this.cachedGoogleFonts[d] = a;
                    b += a;
                    c--;
                    e()
                }), mxUtils.bind(this, function(a) {
                    c--;
                    b += "@import url(" + d + ");";
                    e()
                }))) : b += this.cachedGoogleFonts[d] : b += '@font-face {font-family: "' + a + '";src: url("' + d + '");}'
            })(d[g].name, d[g].url);
            e()
        } else a()
    };
    Editor.prototype.addMathCss = function(a) {
        a = a.getElementsByTagName("defs");
        if (null != a && 0 < a.length)
            for (var d = document.getElementsByTagName("style"), b = 0; b < d.length; b++) 0 < mxUtils.getTextContent(d[b]).indexOf("MathJax") && a[0].appendChild(d[b].cloneNode(!0))
    };
    Editor.prototype.addFontCss = function(a, d) {
        d = null != d ? d : this.absoluteCssFonts(this.fontCss);
        if (null != d) {
            var b = a.getElementsByTagName("defs"),
                c = a.ownerDocument;
            0 == b.length ? (b = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "defs") : c.createElement("defs"), null != a.firstChild ? a.insertBefore(b, a.firstChild) : a.appendChild(b)) : b = b[0];
            c = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "style") : c.createElement("style");
            c.setAttribute("type", "text/css");
            mxUtils.setTextContent(c, d);
            b.appendChild(c)
        }
    };
    Editor.prototype.isExportToCanvas = function() {
        return mxClient.IS_CHROMEAPP || this.useCanvasForExport
    };
    Editor.prototype.exportToCanvas = function(a, d, b, c, e, g, m, f, n, p, k, l, t, z, u, q) {
        try {
            g = null != g ? g : !0;
            m = null != m ? m : !0;
            l = null != l ? l : this.graph;
            t = null != t ? t : 0;
            var v = n ? null : l.background;
            v == mxConstants.NONE && (v = null);
            null == v && (v = c);
            null == v && 0 == n && (v = q ? this.graph.defaultPageBackgroundColor : "#ffffff");
            this.convertImages(l.getSvg(null, null, t, z, null, m, null, null, null, p, null, q), mxUtils.bind(this, function(b) {
                try {
                    var c = new Image;
                    c.onload = mxUtils.bind(this, function() {
                        try {
                            var m = function() {
                                    mxClient.IS_SF ? window.setTimeout(function() {
                                        z.drawImage(c, 0, 0);
                                        a(n)
                                    }, 0) : (z.drawImage(c, 0, 0), a(n))
                                },
                                n = document.createElement("canvas"),
                                p = parseInt(b.getAttribute("width")),
                                k = parseInt(b.getAttribute("height"));
                            f = null != f ? f : 1;
                            null != d && (f = g ? Math.min(1, Math.min(3 * d / (4 * k), d / p)) : d / p);
                            p = Math.ceil(f * p);
                            k = Math.ceil(f * k);
                            n.setAttribute("width", p);
                            n.setAttribute("height", k);
                            var z = n.getContext("2d");
                            null != v && (z.beginPath(), z.rect(0, 0, p, k), z.fillStyle = v, z.fill());
                            z.scale(f, f);
                            if (u) {
                                var q = l.view,
                                    B = q.scale;
                                q.scale = 1;
                                var x = btoa(unescape(encodeURIComponent(q.createSvgGrid(q.gridColor))));
                                q.scale = B;
                                var x = "data:image/svg+xml;base64," + x,
                                    C = l.gridSize * q.gridSteps * f,
                                    I = l.getGraphBounds(),
                                    G = q.translate.x * B,
                                    y = q.translate.y * B,
                                    D = G + (I.x - G) / B - t,
                                    K = y + (I.y - y) / B - t,
                                    A = new Image;
                                A.onload = function() {
                                    try {
                                        for (var a = -Math.round(C - mxUtils.mod((G - D) * f, C)), d = -Math.round(C - mxUtils.mod((y - K) * f, C)); a < p; a += C)
                                            for (var b = d; b < k; b += C) z.drawImage(A, a / f, b / f);
                                        m()
                                    } catch (na) {
                                        null != e && e(na)
                                    }
                                };
                                A.onerror = function(a) {
                                    null != e && e(a)
                                };
                                A.src = x
                            } else m()
                        } catch (la) {
                            null != e && e(la)
                        }
                    });
                    c.onerror = function(a) {
                        null != e && e(a)
                    };
                    p && this.graph.addSvgShadow(b);
                    this.graph.mathEnabled && this.addMathCss(b);
                    var m = mxUtils.bind(this, function() {
                        try {
                            null != this.resolvedFontCss && this.addFontCss(b, this.resolvedFontCss), c.src = Editor.createSvgDataUri(mxUtils.getXml(b))
                        } catch (R) {
                            null != e && e(R)
                        }
                    });
                    this.embedExtFonts(mxUtils.bind(this, function(a) {
                        try {
                            null != a && this.addFontCss(b, a), this.loadFonts(m)
                        } catch (X) {
                            null != e && e(X)
                        }
                    }))
                } catch (R) {
                    null != e && e(R)
                }
            }), b, k)
        } catch (Q) {
            null != e && e(Q)
        }
    };
    Editor.crcTable = [];
    for (var k = 0; 256 > k; k++)
        for (var f = k, l = 0; 8 > l; l++) f = 1 == (f & 1) ? 3988292384 ^ f >>> 1 : f >>> 1, Editor.crcTable[k] = f;
    Editor.updateCRC = function(a, d, b, c) {
        for (var e = 0; e < c; e++) a = Editor.crcTable[(a ^ d.charCodeAt(b + e)) & 255] ^ a >>> 8;
        return a
    };
    Editor.crc32 = function(a) {
        for (var d = -1, b = 0; b < a.length; b++) d = d >>> 8 ^ Editor.crcTable[(d ^ a.charCodeAt(b)) & 255];
        return (d ^ -1) >>> 0
    };
    Editor.writeGraphModelToPng = function(a, d, b, c, e) {
        function g(a, d) {
            var b = n;
            n += d;
            return a.substring(b, n)
        }

        function m(a) {
            a = g(a, 4);
            return a.charCodeAt(3) + (a.charCodeAt(2) << 8) + (a.charCodeAt(1) << 16) + (a.charCodeAt(0) << 24)
        }

        function f(a) {
            return String.fromCharCode(a >> 24 & 255, a >> 16 & 255, a >> 8 & 255, a & 255)
        }
        a = a.substring(a.indexOf(",") + 1);
        a = window.atob ? atob(a) : Base64.decode(a, !0);
        var n = 0;
        if (g(a, 8) != String.fromCharCode(137) + "PNG" + String.fromCharCode(13, 10, 26, 10)) null != e && e();
        else if (g(a, 4), "IHDR" != g(a, 4)) null != e && e();
        else {
            g(a, 17);
            e = a.substring(0, n);
            do {
                var p = m(a);
                if ("IDAT" == g(a, 4)) {
                    e = a.substring(0, n - 8);
                    "pHYs" == d && "dpi" == b ? (b = Math.round(c / .0254), b = f(b) + f(b) + String.fromCharCode(1)) : b = b + String.fromCharCode(0) + ("zTXt" == d ? String.fromCharCode(0) : "") + c;
                    c = 4294967295;
                    c = Editor.updateCRC(c, d, 0, 4);
                    c = Editor.updateCRC(c, b, 0, b.length);
                    e += f(b.length) + d + b + f(c ^ 4294967295);
                    e += a.substring(n - 8, a.length);
                    break
                }
                e += a.substring(n - 8, n - 4 + p);
                g(a, p);
                g(a, 4)
            } while (p);
            return "data:image/png;base64," + (window.btoa ? btoa(e) : Base64.encode(e, !0))
        }
    };
    // if (window.ColorDialog) {
    //   FilenameDialog.filenameHelpLink = "https://desk.draw.io/support/solutions/articles/16000091426";
    //   var d = ColorDialog.addRecentColor;
    //   ColorDialog.addRecentColor = function (a, b) {
    //     d.apply(this, arguments);
    //     mxSettings.setRecentColors(ColorDialog.recentColors);
    //     mxSettings.save()
    //   };
    //   var g = ColorDialog.resetRecentColors;
    //   ColorDialog.resetRecentColors = function () {
    //     g.apply(this, arguments);
    //     mxSettings.setRecentColors(ColorDialog.recentColors);
    //     mxSettings.save()
    //   }
    // }
    window.EditDataDialog && (EditDataDialog.getDisplayIdForCell = function(a, d) {
        var b = null;
        null != a.editor.graph.getModel().getParent(d) ? b = d.getId() : null != a.currentPage && (b = a.currentPage.getId());
        return b
    });
    if (null != window.StyleFormatPanel) {
        var m = Format.prototype.init;
        Format.prototype.init = function() {
            m.apply(this, arguments);
            this.editorUi.editor.addListener("fileLoaded", this.update)
        };
        var n = Format.prototype.refresh;
        Format.prototype.refresh = function() {
            null != this.editorUi.getCurrentFile() || "1" == urlParams.embed || this.editorUi.editor.chromeless ? n.apply(this, arguments) : this.clear()
        };
        DiagramFormatPanel.prototype.isShadowOptionVisible = function() {
            var a = this.editorUi.getCurrentFile();
            return "1" == urlParams.embed || null != a && a.isEditable()
        };
        DiagramFormatPanel.prototype.isMathOptionVisible = function(a) {
            return !1
        };
        var p = DiagramFormatPanel.prototype.addView;
        DiagramFormatPanel.prototype.addView = function(a) {
            a = p.apply(this, arguments);
            this.editorUi.getCurrentFile();
            if (mxClient.IS_SVG && this.isShadowOptionVisible()) {
                var d = this.editorUi,
                    b = d.editor.graph,
                    c = this.createOption(mxResources.get("shadow"), function() {
                        return b.shadowVisible
                    }, function(a) {
                        var c = new ChangePageSetup(d);
                        c.ignoreColor = !0;
                        c.ignoreImage = !0;
                        c.shadowVisible = a;
                        b.model.execute(c)
                    }, {
                        install: function(a) {
                            this.listener = function() {
                                a(b.shadowVisible)
                            };
                            d.addListener("shadowVisibleChanged", this.listener)
                        },
                        destroy: function() {
                            d.removeListener(this.listener)
                        }
                    });
                Editor.shadowOptionEnabled || (c.getElementsByTagName("input")[0].setAttribute("disabled", "disabled"), mxUtils.setOpacity(c, 60));
                a.appendChild(c)
            }
            return a
        };
        var t = DiagramFormatPanel.prototype.addOptions;
        DiagramFormatPanel.prototype.addOptions = function(a) {
            a = t.apply(this, arguments);
            var d = this.editorUi,
                b = d.editor.graph;
            if (b.isEnabled()) {
                var c = d.getCurrentFile();
                if (null != c && c.isAutosaveOptional()) {
                    var e = this.createOption(mxResources.get("autosave"), function() {
                        return d.editor.autosave
                    }, function(a) {
                        d.editor.setAutosave(a);
                        d.editor.autosave && c.isModified() && c.fileChanged()
                    }, {
                        install: function(a) {
                            this.listener = function() {
                                a(d.editor.autosave)
                            };
                            d.editor.addListener("autosaveChanged", this.listener)
                        },
                        destroy: function() {
                            d.editor.removeListener(this.listener)
                        }
                    });
                    a.appendChild(e)
                }
            }
            if (this.isMathOptionVisible() && b.isEnabled() && "undefined" !== typeof MathJax) {
                e = this.createOption(mxResources.get("mathematicalTypesetting"), function() {
                    return b.mathEnabled
                }, function(a) {
                    d.actions.get("mathematicalTypesetting").funct()
                }, {
                    install: function(a) {
                        this.listener = function() {
                            a(b.mathEnabled)
                        };
                        d.addListener("mathEnabledChanged", this.listener)
                    },
                    destroy: function() {
                        d.removeListener(this.listener)
                    }
                });
                e.style.paddingTop = "5px";
                a.appendChild(e);
                var g = d.menus.createHelpLink("https://desk.draw.io/support/solutions/articles/16000032875");
                g.style.position = "relative";
                g.style.marginLeft = "6px";
                g.style.top = "2px";
                e.appendChild(g)
            }
            return a
        };
        mxCellRenderer.prototype.defaultVertexShape.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "absoluteArcSize",
            dispName: "Abs. Arc Size",
            type: "bool",
            defVal: !1
        }];
        mxCellRenderer.defaultShapes.link.prototype.customProperties = [{
            name: "width",
            dispName: "Width",
            type: "float",
            min: 0,
            defVal: 4
        }];
        mxCellRenderer.defaultShapes.flexArrow.prototype.customProperties = [{
            name: "width",
            dispName: "Width",
            type: "float",
            min: 0,
            defVal: 10
        }, {
            name: "startWidth",
            dispName: "Start Width",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "endWidth",
            dispName: "End Width",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.process.prototype.customProperties = [{
            name: "size",
            dispName: "Indent",
            type: "float",
            min: 0,
            max: .5,
            defVal: .1
        }];
        mxCellRenderer.defaultShapes.rhombus.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            max: 50,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "double",
            dispName: "Double",
            type: "bool",
            defVal: !1
        }];
        mxCellRenderer.defaultShapes.partialRectangle.prototype.customProperties = [{
            name: "top",
            dispName: "Top Line",
            type: "bool",
            defVal: !0
        }, {
            name: "bottom",
            dispName: "Bottom Line",
            type: "bool",
            defVal: !0
        }, {
            name: "left",
            dispName: "Left Line",
            type: "bool",
            defVal: !0
        }, {
            name: "right",
            dispName: "Right Line",
            type: "bool",
            defVal: !0
        }];
        mxCellRenderer.defaultShapes.parallelogram.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "size",
            dispName: "Slope Angle",
            type: "float",
            min: 0,
            max: 1,
            defVal: .2
        }];
        mxCellRenderer.defaultShapes.hexagon.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "size",
            dispName: "Slope Angle",
            type: "float",
            min: 0,
            max: 1,
            defVal: .25
        }];
        mxCellRenderer.defaultShapes.triangle.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }];
        mxCellRenderer.defaultShapes.document.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            defVal: .3,
            min: 0,
            max: 1
        }];
        mxCellRenderer.defaultShapes.internalStorage.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "dx",
            dispName: "Left Line",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "dy",
            dispName: "Top Line",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.cube.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "darkOpacity",
            dispName: "Dark Opacity",
            type: "float",
            min: -1,
            max: 1,
            defVal: 0
        }, {
            name: "darkOpacity2",
            dispName: "Dark Opacity 2",
            type: "float",
            min: -1,
            max: 1,
            defVal: 0
        }];
        mxCellRenderer.defaultShapes.step.prototype.customProperties = [{
            name: "size",
            dispName: "Notch Size",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "fixedSize",
            dispName: "Fixed Size",
            type: "bool",
            defVal: !0
        }];
        mxCellRenderer.defaultShapes.trapezoid.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "size",
            dispName: "Slope Angle",
            type: "float",
            min: 0,
            max: 1,
            defVal: .2
        }];
        mxCellRenderer.defaultShapes.tape.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            max: 1,
            defVal: .4
        }];
        mxCellRenderer.defaultShapes.note.prototype.customProperties = [{
            name: "size",
            dispName: "Fold Size",
            type: "float",
            min: 0,
            defVal: 30
        }, {
            name: "darkOpacity",
            dispName: "Dark Opacity",
            type: "float",
            min: -1,
            max: 1,
            defVal: 0
        }];
        mxCellRenderer.defaultShapes.card.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "size",
            dispName: "Cutoff Size",
            type: "float",
            min: 0,
            defVal: 30
        }];
        mxCellRenderer.defaultShapes.callout.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: mxConstants.LINE_ARCSIZE
        }, {
            name: "base",
            dispName: "Callout Width",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "size",
            dispName: "Callout Length",
            type: "float",
            min: 0,
            defVal: 30
        }, {
            name: "position",
            dispName: "Callout Position",
            type: "float",
            min: 0,
            max: 1,
            defVal: .5
        }, {
            name: "position2",
            dispName: "Callout Tip Position",
            type: "float",
            min: 0,
            max: 1,
            defVal: .5
        }];
        mxCellRenderer.defaultShapes.folder.prototype.customProperties = [{
            name: "tabWidth",
            dispName: "Tab Width",
            type: "float"
        }, {
            name: "tabHeight",
            dispName: "Tab Height",
            type: "float"
        }, {
            name: "tabPosition",
            dispName: "Tap Position",
            type: "enum",
            enumList: [{
                val: "left",
                dispName: "Left"
            }, {
                val: "right",
                dispName: "Right"
            }]
        }];
        mxCellRenderer.defaultShapes.swimlane.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 15
        }, {
            name: "startSize",
            dispName: "Header Size",
            type: "float"
        }, {
            name: "horizontal",
            dispName: "Horizontal",
            type: "bool",
            defVal: !0
        }, {
            name: "separatorColor",
            dispName: "Separator Color",
            type: "color",
            defVal: null
        }];
        mxCellRenderer.defaultShapes.table.prototype.customProperties = [{
            name: "rowLines",
            dispName: "Row Lines",
            type: "bool",
            defVal: !0
        }, {
            name: "columnLines",
            dispName: "Column Lines",
            type: "bool",
            defVal: !0
        }, {
            name: "fixedRows",
            dispName: "Fixed Rows",
            type: "bool",
            defVal: !1
        }, {
            name: "resizeLast",
            dispName: "Resize Last Column",
            type: "bool",
            defVal: !1
        }, {
            name: "resizeLastRow",
            dispName: "Resize Last Row",
            type: "bool",
            defVal: !1
        }].concat(mxCellRenderer.defaultShapes.swimlane.prototype.customProperties);
        mxCellRenderer.defaultShapes.doubleEllipse.prototype.customProperties = [{
            name: "margin",
            dispName: "Indent",
            type: "float",
            min: 0,
            defVal: 4
        }];
        mxCellRenderer.defaultShapes.ext.prototype.customProperties = [{
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 15
        }, {
            name: "double",
            dispName: "Double",
            type: "bool",
            defVal: !1
        }, {
            name: "margin",
            dispName: "Indent",
            type: "float",
            min: 0,
            defVal: 0
        }];
        mxCellRenderer.defaultShapes.curlyBracket.prototype.customProperties = [{
            name: "rounded",
            dispName: "Rounded",
            type: "bool",
            defVal: !0
        }, {
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            max: 1,
            defVal: .5
        }];
        mxCellRenderer.defaultShapes.image.prototype.customProperties = [{
            name: "imageAspect",
            dispName: "Fixed Image Aspect",
            type: "bool",
            defVal: !0
        }];
        mxCellRenderer.defaultShapes.label.prototype.customProperties = [{
            name: "imageAspect",
            dispName: "Fixed Image Aspect",
            type: "bool",
            defVal: !0
        }, {
            name: "imageAlign",
            dispName: "Image Align",
            type: "enum",
            enumList: [{
                val: "left",
                dispName: "Left"
            }, {
                val: "center",
                dispName: "Center"
            }, {
                val: "right",
                dispName: "Right"
            }],
            defVal: "left"
        }, {
            name: "imageVerticalAlign",
            dispName: "Image Vertical Align",
            type: "enum",
            enumList: [{
                val: "top",
                dispName: "Top"
            }, {
                val: "middle",
                dispName: "Middle"
            }, {
                val: "bottom",
                dispName: "Bottom"
            }],
            defVal: "middle"
        }, {
            name: "imageWidth",
            dispName: "Image Width",
            type: "float",
            min: 0,
            defVal: 24
        }, {
            name: "imageHeight",
            dispName: "Image Height",
            type: "float",
            min: 0,
            defVal: 24
        }, {
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 12
        }, {
            name: "absoluteArcSize",
            dispName: "Abs. Arc Size",
            type: "bool",
            defVal: !1
        }];
        mxCellRenderer.defaultShapes.dataStorage.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            max: 1,
            defVal: .1
        }];
        mxCellRenderer.defaultShapes.manualInput.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            defVal: 30
        }, {
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.loopLimit.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.offPageConnector.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            defVal: 38
        }, {
            name: "arcSize",
            dispName: "Arc Size",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.display.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            max: 1,
            defVal: .25
        }];
        mxCellRenderer.defaultShapes.singleArrow.prototype.customProperties = [{
            name: "arrowWidth",
            dispName: "Arrow Width",
            type: "float",
            min: 0,
            max: 1,
            defVal: .3
        }, {
            name: "arrowSize",
            dispName: "Arrowhead Length",
            type: "float",
            min: 0,
            max: 1,
            defVal: .2
        }];
        mxCellRenderer.defaultShapes.doubleArrow.prototype.customProperties = [{
            name: "arrowWidth",
            dispName: "Arrow Width",
            type: "float",
            min: 0,
            max: 1,
            defVal: .3
        }, {
            name: "arrowSize",
            dispName: "Arrowhead Length",
            type: "float",
            min: 0,
            max: 1,
            defVal: .2
        }];
        mxCellRenderer.defaultShapes.cross.prototype.customProperties = [{
            name: "size",
            dispName: "Size",
            type: "float",
            min: 0,
            max: 1,
            defVal: .2
        }];
        mxCellRenderer.defaultShapes.corner.prototype.customProperties = [{
            name: "dx",
            dispName: "Width1",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "dy",
            dispName: "Width2",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.tee.prototype.customProperties = [{
            name: "dx",
            dispName: "Width1",
            type: "float",
            min: 0,
            defVal: 20
        }, {
            name: "dy",
            dispName: "Width2",
            type: "float",
            min: 0,
            defVal: 20
        }];
        mxCellRenderer.defaultShapes.umlLifeline.prototype.customProperties = [{
            name: "participant",
            dispName: "Participant",
            type: "enum",
            defVal: "none",
            enumList: [{
                val: "none",
                dispName: "Default"
            }, {
                val: "umlActor",
                dispName: "Actor"
            }, {
                val: "umlBoundary",
                dispName: "Boundary"
            }, {
                val: "umlEntity",
                dispName: "Entity"
            }, {
                val: "umlControl",
                dispName: "Control"
            }]
        }, {
            name: "size",
            dispName: "Height",
            type: "float",
            defVal: 40,
            min: 0
        }];
        mxCellRenderer.defaultShapes.umlFrame.prototype.customProperties = [{
            name: "width",
            dispName: "Title Width",
            type: "float",
            defVal: 60,
            min: 0
        }, {
            name: "height",
            dispName: "Title Height",
            type: "float",
            defVal: 30,
            min: 0
        }];
        StyleFormatPanel.prototype.defaultColorSchemes = [
            [{
                fill: "",
                stroke: ""
            }, {
                fill: "#f5f5f5",
                stroke: "#666666",
                font: "#333333"
            }, {
                fill: "#dae8fc",
                stroke: "#6c8ebf"
            }, {
                fill: "#d5e8d4",
                stroke: "#82b366"
            }, {
                fill: "#ffe6cc",
                stroke: "#d79b00"
            }, {
                fill: "#fff2cc",
                stroke: "#d6b656"
            }, {
                fill: "#f8cecc",
                stroke: "#b85450"
            }, {
                fill: "#e1d5e7",
                stroke: "#9673a6"
            }],
            [{
                fill: "",
                stroke: ""
            }, {
                fill: "#60a917",
                stroke: "#2D7600",
                font: "#ffffff"
            }, {
                fill: "#008a00",
                stroke: "#005700",
                font: "#ffffff"
            }, {
                fill: "#2EBAC9",
                stroke: "#006EAF",
                font: "#ffffff"
            }, {
                fill: "#3E54E6",
                stroke: "#001DBC",
                font: "#ffffff"
            }, {
                fill: "#6a00ff",
                stroke: "#3700CC",
                font: "#ffffff"
            }, {
                fill: "#C93AC9",
                stroke: "#A50040",
                font: "#ffffff"
            }, {
                fill: "#8F4132",
                stroke: "#6F0000",
                font: "#ffffff"
            }],
            [{
                fill: "#e51400",
                stroke: "#B20000",
                font: "#ffffff"
            }, {
                fill: "#fa6800",
                stroke: "#C73500",
                font: "#ffffff"
            }, {
                fill: "#f0a30a",
                stroke: "#BD7000",
                font: "#ffffff"
            }, {
                fill: "#D1BC35",
                stroke: "#B09500",
                font: "#ffffff"
            }, {
                fill: "#6d8764",
                stroke: "#3A5431",
                font: "#ffffff"
            }, {
                fill: "#647687",
                stroke: "#314354",
                font: "#ffffff"
            }, {
                fill: "#76608a",
                stroke: "#432D57",
                font: "#ffffff"
            }, {
                fill: "#a0522d",
                stroke: "#6D1F00",
                font: "#ffffff"
            }],
            [{
                fill: "",
                stroke: ""
            }, {
                fill: mxConstants.NONE,
                stroke: ""
            }, {
                fill: "#fad7ac",
                stroke: "#b46504"
            }, {
                fill: "#fad9d5",
                stroke: "#ae4132"
            }, {
                fill: "#b0e3e6",
                stroke: "#0e8088"
            }, {
                fill: "#b1ddf0",
                stroke: "#10739e"
            }, {
                fill: "#d0cee2",
                stroke: "#56517e"
            }, {
                fill: "#bac8d3",
                stroke: "#23445d"
            }],
            [{
                fill: "",
                stroke: ""
            }, {
                fill: "#f5f5f5",
                stroke: "#666666",
                gradient: "#b3b3b3"
            }, {
                fill: "#dae8fc",
                stroke: "#6c8ebf",
                gradient: "#7ea6e0"
            }, {
                fill: "#d5e8d4",
                stroke: "#82b366",
                gradient: "#97d077"
            }, {
                fill: "#ffcd28",
                stroke: "#d79b00",
                gradient: "#ffa500"
            }, {
                fill: "#fff2cc",
                stroke: "#d6b656",
                gradient: "#ffd966"
            }, {
                fill: "#f8cecc",
                stroke: "#b85450",
                gradient: "#ea6b66"
            }, {
                fill: "#e6d0de",
                stroke: "#996185",
                gradient: "#d5739d"
            }],
            [{
                fill: "",
                stroke: ""
            }, {
                fill: "#eeeeee",
                stroke: "#36393d"
            }, {
                fill: "#f9f7ed",
                stroke: "#36393d"
            }, {
                fill: "#ffcc99",
                stroke: "#36393d"
            }, {
                fill: "#cce5ff",
                stroke: "#36393d"
            }, {
                fill: "#ffff88",
                stroke: "#36393d"
            }, {
                fill: "#cdeb8b",
                stroke: "#36393d"
            }, {
                fill: "#ffcccc",
                stroke: "#36393d"
            }]
        ];
        StyleFormatPanel.prototype.customColorSchemes = null;
        StyleFormatPanel.prototype.findCommonProperties = function(a, d, b) {
            if (null != d) {
                var c = function(a) {
                        if (null != a)
                            if (b)
                                for (var c = 0; c < a.length; c++) d[a[c].name] = a[c];
                            else
                                for (var e in d) {
                                    for (var g = !1, c = 0; c < a.length; c++)
                                        if (a[c].name == e && a[c].type == d[e].type) {
                                            g = !0;
                                            break
                                        }
                                    g || delete d[e]
                                }
                    },
                    e = this.editorUi.editor.graph.view.getState(a);
                null != e && null != e.shape && (e.shape.commonCustomPropAdded || (e.shape.commonCustomPropAdded = !0, e.shape.customProperties = e.shape.customProperties || [], e.cell.vertex ? Array.prototype.push.apply(e.shape.customProperties, Editor.commonVertexProperties) : Array.prototype.push.apply(e.shape.customProperties, Editor.commonEdgeProperties)), c(e.shape.customProperties));
                a = a.getAttribute("customProperties");
                if (null != a) try {
                    c(JSON.parse(a))
                } catch (J) {}
            }
        };

        var q = StyleFormatPanel.prototype.addStyleOps;
        StyleFormatPanel.prototype.addStyleOps = function(a) {

            d = mxUtils.button(mxResources.get("editseeAlso"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("editseeAlso").funct()
            }));
            d.setAttribute("title", mxResources.get("editseeAlso") + " (" + this.editorUi.actions.get("editseeAlso").shortcut + ")");
            d.style.marginBottom = "9px";
            d.style.width = "202px";
            d.style.height = "20px";
            d.style.backgroundColor = "rgb(79 79 79)";
            d.style.borderColor = "rgb(37 37 37)";
            d.style.color = "white";
            // d.style.backgroundColor="rgb(255 255 255)";
            // d.style.borderColor="rgb(37 37 37)";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setProblemSolvingLayerStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setProblemSolvingLayerStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setProblemSolvingLayerStyle") + " (" + this.editorUi.actions.get("setProblemSolvingLayerStyle").shortcut + ")");
            d.style.marginBottom = "2px";
            d.style.width = "202px";
            d.style.height = "20px";
            d.style.backgroundColor = "#FFE6CC";
            d.style.borderColor = "#d79b00";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setInformationLayerStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setInformationLayerStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setInformationLayerStyle") + " (" + this.editorUi.actions.get("setInformationLayerStyle").shortcut + ")");
            d.style.marginBottom = "2px";
            d.style.width = "202px";
            d.style.height = "20px";
            d.style.backgroundColor = "#dae8fc";
            d.style.borderColor = "#6c8ebf";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setPhysicalLayerStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setPhysicalLayerStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setPhysicalLayerStyle") + " (" + this.editorUi.actions.get("setPhysicalLayerStyle").shortcut + ")");
            d.style.marginBottom = "2px";
            d.style.width = "202px";
            d.style.height = "20px";
            d.style.backgroundColor = "#d5e8d4";
            d.style.borderColor = "#82b366";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setDPBoxStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setDPBoxStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setDPBoxStyle") + " (" + this.editorUi.actions.get("setDPBoxStyle").shortcut + ")");
            d.style.marginBottom = "1px";
            d.style.width = "202px";
            d.style.color = "#ffffff";
            d.style.borderColor = "#d79b00";
            d.style.backgroundColor = "#D1BC35";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setCAIBoxStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setCAIBoxStyle").funct()
            }));
            // d.setAttribute("title", mxResources.get("setCAIBoxStyle") + " (" + this.editorUi.actions.get("setCAIBoxStyle").shortcut + ")");
            d.style.marginBottom = "1px";
            d.style.width = "202px";
            d.style.color = "#ffffff";
            d.style.borderColor = "#d79b00";
            d.style.backgroundColor = "#3E54E6";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setGHBoxStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setGHBoxStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setGHBoxStyle") + " (" + this.editorUi.actions.get("setGHBoxStyle").shortcut + ")");
            d.style.marginBottom = "1px";
            d.style.width = "202px";
            d.style.color = "#ffffff";
            d.style.borderColor = "#d79b00";
            d.style.backgroundColor = "#C93AC9";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setESIBoxStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setESIBoxStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setESIBoxStyle") + " (" + this.editorUi.actions.get("setESIBoxStyle").shortcut + ")");
            d.style.marginBottom = "1px";
            d.style.width = "202px";
            d.style.color = "#ffffff";
            d.style.borderColor = "#d79b00";
            d.style.backgroundColor = "#8F4132";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            d = mxUtils.button(mxResources.get("setEXBoxStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("setEXBoxStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("setEXBoxStyle") + " (" + this.editorUi.actions.get("setEXBoxStyle").shortcut + ")");
            d.style.marginBottom = "10px";
            d.style.width = "202px";
            d.style.color = "#ffffff";
            d.style.borderColor = "#d79b00";
            d.style.backgroundColor = "#2EBAC9";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);
            mxUtils.br(a);

            var d = mxUtils.button(mxResources.get("copyStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("copyStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("copyStyle") + " (" + this.editorUi.actions.get("copyStyle").shortcut + ")");
            d.style.marginBottom = "2px";
            d.style.width = "100px";
            d.style.marginRight = "2px";
            d.style.height = "20px";
            d.style.backgroundColor = "rgb(242 242 242)";
            d.style.borderColor = "rgb(37 37 37)";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);

            d = mxUtils.button(mxResources.get("pasteStyle"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("pasteStyle").funct()
            }));
            d.setAttribute("title", mxResources.get("pasteStyle") + " (" + this.editorUi.actions.get("pasteStyle").shortcut + ")");
            d.style.marginBottom = "2px";
            d.style.width = "100px";
            d.style.height = "20px";
            d.style.backgroundColor = "rgb(242 242 242)";
            d.style.borderColor = "rgb(37 37 37)";
            d.style.borderRadius = "3px";
            d.style.borderWidth = "thin";
            a.appendChild(d);

            return q.apply(this, arguments)
        };
        EditorUi.prototype.propertiesCollapsed = !0;
        StyleFormatPanel.prototype.addProperties = function(a, d, b) {
            function c(a, d, b, c) {
                l.getModel().beginUpdate();
                try {
                    var e = [],
                        g = [];
                    if (null != b.index) {
                        for (var m = [], f = b.parentRow.nextSibling; f && f.getAttribute("data-pName") == a;) m.push(f.getAttribute("data-pValue")), f = f.nextSibling;
                        b.index < m.length ? null != c ? m.splice(c, 1) : m[b.index] = d : m.push(d);
                        null != b.size && m.length > b.size && (m = m.slice(0, b.size));
                        d = m.join(",");
                        null != b.countProperty && (l.setCellStyles(b.countProperty, m.length, l.getSelectionCells()), e.push(b.countProperty), g.push(m.length))
                    }
                    l.setCellStyles(a, d, l.getSelectionCells());
                    e.push(a);
                    g.push(d);
                    if (null != b.dependentProps)
                        for (a = 0; a < b.dependentProps.length; a++) {
                            var n = b.dependentPropsDefVal[a],
                                p = b.dependentPropsVals[a];
                            if (p.length > d) p = p.slice(0, d);
                            else
                                for (var t = p.length; t < d; t++) p.push(n);
                            p = p.join(",");
                            l.setCellStyles(b.dependentProps[a], p, l.getSelectionCells());
                            e.push(b.dependentProps[a]);
                            g.push(p)
                        }
                    if ("function" == typeof b.onChange) b.onChange(l, d);
                    k.editorUi.fireEvent(new mxEventObject("styleChanged", "keys", e, "values", g, "cells", l.getSelectionCells()))
                } finally {
                    l.getModel().endUpdate()
                }
            }

            function e(d, b, c) {
                var e = mxUtils.getOffset(a, !0),
                    g = mxUtils.getOffset(d, !0);
                b.style.position = "absolute";
                b.style.left = g.x - e.x + "px";
                b.style.top = g.y - e.y + "px";
                b.style.width = d.offsetWidth + "px";
                b.style.height = d.offsetHeight - (c ? 4 : 0) + "px";
                b.style.zIndex = 5
            }

            function g(a, d, b) {
                var e = document.createElement("div");
                e.style.width = "32px";
                e.style.height = "4px";
                e.style.margin = "2px";
                e.style.border = "1px solid black";
                e.style.background = d && "none" != d ? d : "url('" + Dialog.prototype.noColorImage + "')";
                btn = mxUtils.button("", mxUtils.bind(k, function(g) {
                    this.editorUi.pickColor(d, function(d) {
                        e.style.background = "none" == d ? "url('" + Dialog.prototype.noColorImage + "')" : d;
                        c(a, d, b)
                    });
                    mxEvent.consume(g)
                }));
                btn.style.height = "12px";
                btn.style.width = "40px";
                btn.className = "geColorBtn";
                btn.appendChild(e);
                return btn
            }

            function m(a, d, b, e, g, m, f) {
                null != d && (d = d.split(","), t.push({
                    name: a,
                    values: d,
                    type: b,
                    defVal: e,
                    countProperty: g,
                    parentRow: m,
                    isDeletable: !0,
                    flipBkg: f
                }));
                btn = mxUtils.button("+", mxUtils.bind(k, function(d) {
                    for (var n = m, k = 0; null != n.nextSibling;)
                        if (n.nextSibling.getAttribute("data-pName") == a) n = n.nextSibling, k++;
                        else break;
                    var l = {
                            type: b,
                            parentRow: m,
                            index: k,
                            isDeletable: !0,
                            defVal: e,
                            countProperty: g
                        },
                        k = p(a, "", l, 0 == k % 2, f);
                    c(a, e, l);
                    n.parentNode.insertBefore(k, n.nextSibling);
                    mxEvent.consume(d)
                }));
                btn.style.height = "16px";
                btn.style.width = "25px";
                btn.className = "geColorBtn";
                return btn
            }

            function f(a, d, b, c, e, g, m) {
                if (0 < e) {
                    var f = Array(e);
                    d = null != d ? d.split(",") : [];
                    for (var n = 0; n < e; n++) f[n] = null != d[n] ? d[n] : null != c ? c : "";
                    t.push({
                        name: a,
                        values: f,
                        type: b,
                        defVal: c,
                        parentRow: g,
                        flipBkg: m,
                        size: e
                    })
                }
                return document.createElement("div")
            }

            function n(a, d, b) {
                var e = document.createElement("input");
                e.type = "checkbox";
                e.checked = "1" == d;
                mxEvent.addListener(e, "change", function() {
                    c(a, e.checked ? "1" : "0", b)
                });
                return e
            }

            function p(d, b, p, l, t) {
                var z = p.dispName,
                    u = p.type,
                    q = document.createElement("tr");
                q.className = "gePropRow" + (t ? "Dark" : "") + (l ? "Alt" : "") + " gePropNonHeaderRow";
                q.setAttribute("data-pName", d);
                q.setAttribute("data-pValue", b);
                l = !1;
                null != p.index && (q.setAttribute("data-index", p.index), z = (null != z ? z : "") + "[" + p.index + "]", l = !0);
                var v = document.createElement("td");
                v.className = "gePropRowCell";
                v.innerHTML = mxUtils.htmlEntities(mxResources.get(z, null, z));
                l && (v.style.textAlign = "right");
                q.appendChild(v);
                v = document.createElement("td");
                v.className = "gePropRowCell";
                if ("color" == u) v.appendChild(g(d, b, p));
                else if ("bool" == u || "boolean" == u) v.appendChild(n(d, b, p));
                else if ("enum" == u) {
                    var B = p.enumList;
                    for (t = 0; t < B.length; t++)
                        if (z = B[t], z.val == b) {
                            v.innerHTML = mxUtils.htmlEntities(mxResources.get(z.dispName, null, z.dispName));
                            break
                        }
                    mxEvent.addListener(v, "click", mxUtils.bind(k, function() {
                        var g = document.createElement("select");
                        e(v, g);
                        for (var m = 0; m < B.length; m++) {
                            var f = B[m],
                                n = document.createElement("option");
                            n.value = mxUtils.htmlEntities(f.val);
                            n.innerHTML = mxUtils.htmlEntities(mxResources.get(f.dispName, null, f.dispName));
                            g.appendChild(n)
                        }
                        g.value = b;
                        a.appendChild(g);
                        mxEvent.addListener(g, "change", function() {
                            var a = mxUtils.htmlEntities(g.value);
                            c(d, a, p)
                        });
                        g.focus();
                        mxEvent.addListener(g, "blur", function() {
                            a.removeChild(g)
                        })
                    }))
                } else "dynamicArr" == u ? v.appendChild(m(d, b, p.subType, p.subDefVal, p.countProperty, q, t)) : "staticArr" == u ? v.appendChild(f(d, b, p.subType, p.subDefVal, p.size, q, t)) : (v.innerHTML = b, mxEvent.addListener(v, "click", mxUtils.bind(k, function() {
                    function g() {
                        var a = m.value,
                            a = 0 == a.length && "string" != u ? 0 : a;
                        p.allowAuto && (null != a.trim && "auto" == a.trim().toLowerCase() ? (a = "auto", u = "string") : (a = parseFloat(a), a = isNaN(a) ? 0 : a));
                        null != p.min && a < p.min ? a = p.min : null != p.max && a > p.max && (a = p.max);
                        a = mxUtils.htmlEntities(("int" == u ? parseInt(a) : a) + "");
                        c(d, a, p)
                    }
                    var m = document.createElement("input");
                    e(v, m, !0);
                    m.value = b;
                    m.className = "gePropEditor";
                    "int" != u && "float" != u || p.allowAuto || (m.type = "number", m.step = "int" == u ? "1" : "any", null != p.min && (m.min = parseFloat(p.min)), null != p.max && (m.max = parseFloat(p.max)));
                    a.appendChild(m);
                    mxEvent.addListener(m, "keypress", function(a) {
                        13 == a.keyCode && g()
                    });
                    m.focus();
                    mxEvent.addListener(m, "blur", function() {
                        g()
                    })
                })));
                p.isDeletable && (t = mxUtils.button("-", mxUtils.bind(k, function(a) {
                    c(d, "", p, p.index);
                    mxEvent.consume(a)
                })), t.style.height = "16px", t.style.width = "25px", t.style["float"] = "right", t.className = "geColorBtn", v.appendChild(t));
                q.appendChild(v);
                return q
            }
            var k = this,
                l = this.editorUi.editor.graph,
                t = [];
            a.style.position = "relative";
            a.style.padding = "0";
            var z = document.createElement("table");
            z.className = "geProperties";
            z.style.whiteSpace = "nowrap";
            z.style.width = "100%";
            var u = document.createElement("tr");
            u.className = "gePropHeader";
            var q = document.createElement("th");
            q.className = "gePropHeaderCell";
            var v = document.createElement("img");
            v.src = Sidebar.prototype.expandedImage;
            q.appendChild(v);
            mxUtils.write(q, mxResources.get("property"));
            u.style.cursor = "pointer";
            var B = function() {
                var d = z.querySelectorAll(".gePropNonHeaderRow"),
                    b;
                if (k.editorUi.propertiesCollapsed) {
                    v.src = Sidebar.prototype.collapsedImage;
                    b = "none";
                    for (var c = a.childNodes.length - 1; 0 <= c; c--) try {
                        var e = a.childNodes[c],
                            g = e.nodeName.toUpperCase();
                        "INPUT" != g && "SELECT" != g || a.removeChild(e)
                    } catch (ma) {}
                } else v.src = Sidebar.prototype.expandedImage, b = "";
                for (c = 0; c < d.length; c++) d[c].style.display = b
            };
            mxEvent.addListener(u, "click", function() {
                k.editorUi.propertiesCollapsed = !k.editorUi.propertiesCollapsed;
                B()
            });
            u.appendChild(q);
            q = document.createElement("th");
            q.className = "gePropHeaderCell";
            q.innerHTML = mxResources.get("value");
            u.appendChild(q);
            z.appendChild(u);
            var x = !1,
                C = !1,
                y;
            for (y in d)
                if (u = d[y], "function" != typeof u.isVisible || u.isVisible(b, this)) {
                    var I = null != b.style[y] ? mxUtils.htmlEntities(b.style[y] + "") : null != u.getDefaultValue ? u.getDefaultValue(b, this) : u.defVal;
                    if ("separator" == u.type) C = !C;
                    else {
                        if ("staticArr" == u.type) u.size = parseInt(b.style[u.sizeProperty] || d[u.sizeProperty].defVal) || 0;
                        else if (null != u.dependentProps) {
                            for (var G = u.dependentProps, D = [], A = [], q = 0; q < G.length; q++) {
                                var K = b.style[G[q]];
                                A.push(d[G[q]].subDefVal);
                                D.push(null != K ? K.split(",") : [])
                            }
                            u.dependentPropsDefVal = A;
                            u.dependentPropsVals = D
                        }
                        z.appendChild(p(y, I, u, x, C));
                        x = !x
                    }
                }
            for (q = 0; q < t.length; q++)
                for (u = t[q], d = u.parentRow, b = 0; b < u.values.length; b++) y = p(u.name, u.values[b], {
                    type: u.type,
                    parentRow: u.parentRow,
                    isDeletable: u.isDeletable,
                    index: b,
                    defVal: u.defVal,
                    countProperty: u.countProperty,
                    size: u.size
                }, 0 == b % 2, u.flipBkg), d.parentNode.insertBefore(y, d.nextSibling), d = y;
            a.appendChild(z);
            B();
            return a
        };
        StyleFormatPanel.prototype.addStyles = function(a) {
            function d(a) {
                mxEvent.addListener(a, "mouseenter", function() {
                    a.style.opacity = "1"
                });
                mxEvent.addListener(a, "mouseleave", function() {
                    a.style.opacity = "0.5"
                })
            }
            var b = this.editorUi,
                c = b.editor.graph,
                e = document.createElement("div");
            e.style.whiteSpace = "nowrap";
            e.style.paddingLeft = "24px";
            e.style.paddingRight = "20px";
            a.style.paddingLeft = "16px";
            a.style.paddingBottom = "6px";
            a.style.position = "relative";
            a.appendChild(e);
            var g = "plain-gray plain-blue plain-green plain-turquoise plain-orange plain-yellow plain-red plain-pink plain-purple gray blue green turquoise orange yellow red pink purple".split(" "),
                m = document.createElement("div");
            m.style.whiteSpace = "nowrap";
            m.style.position = "relative";
            m.style.textAlign = "center";
            for (var f = [], n = 0; n < this.defaultColorSchemes.length; n++) {
                var p = document.createElement("div");
                p.style.display = "inline-block";
                p.style.width = "6px";
                p.style.height = "6px";
                p.style.marginLeft = "4px";
                p.style.marginRight = "3px";
                p.style.borderRadius = "3px";
                p.style.cursor = "pointer";
                p.style.background = "transparent";
                p.style.border = "1px solid #b5b6b7";
                mxUtils.bind(this, function(a) {
                    mxEvent.addListener(p, "click", mxUtils.bind(this, function() {
                        k(a)
                    }))
                })(n);
                f.push(p);
                m.appendChild(p)
            }
            var k = mxUtils.bind(this, function(a) {
                    null != this.format.currentScheme && (f[this.format.currentScheme].style.background = "transparent");
                    this.format.currentScheme = a;
                    l(this.defaultColorSchemes[this.format.currentScheme]);
                    f[this.format.currentScheme].style.background = "#84d7ff"
                }),
                l = mxUtils.bind(this, function(a) {
                    var d = mxUtils.bind(this, function(a) {
                        var d = mxUtils.button("", function(d) {
                            c.getModel().beginUpdate();
                            try {
                                for (var e = c.getSelectionCells(), m = 0; m < e.length; m++) {
                                    for (var f = c.getModel().getStyle(e[m]), n = 0; n < g.length; n++) f = mxUtils.removeStylename(f, g[n]);
                                    var p = c.getModel().isVertex(e[m]) ? b.initialDefaultVertexStyle : b.initialdefaultEdgeStyle;
                                    null != a ? (f = mxUtils.setStyle(f, mxConstants.STYLE_GRADIENTCOLOR, a.gradient || mxUtils.getValue(p, mxConstants.STYLE_GRADIENTCOLOR, null)), mxEvent.isControlDown(d) || mxClient.IS_MAC && mxEvent.isMetaDown(d) || (f = "" == a.fill ? mxUtils.setStyle(f, mxConstants.STYLE_FILLCOLOR, null) : mxUtils.setStyle(f, mxConstants.STYLE_FILLCOLOR, a.fill || mxUtils.getValue(p, mxConstants.STYLE_FILLCOLOR, null))), mxEvent.isShiftDown(d) || (f = "" == a.stroke ? mxUtils.setStyle(f, mxConstants.STYLE_STROKECOLOR, null) : mxUtils.setStyle(f, mxConstants.STYLE_STROKECOLOR, a.stroke || mxUtils.getValue(p, mxConstants.STYLE_STROKECOLOR, null))), !mxEvent.isAltDown(d) && c.getModel().isVertex(e[m]) && (f = mxUtils.setStyle(f, mxConstants.STYLE_FONTCOLOR, a.font || mxUtils.getValue(p, mxConstants.STYLE_FONTCOLOR, null)))) : (f = mxUtils.setStyle(f, mxConstants.STYLE_FILLCOLOR, mxUtils.getValue(p, mxConstants.STYLE_FILLCOLOR, "#ffffff")), f = mxUtils.setStyle(f, mxConstants.STYLE_STROKECOLOR, mxUtils.getValue(p, mxConstants.STYLE_STROKECOLOR, "#000000")), f = mxUtils.setStyle(f, mxConstants.STYLE_GRADIENTCOLOR, mxUtils.getValue(p, mxConstants.STYLE_GRADIENTCOLOR, null)), c.getModel().isVertex(e[m]) && (f = mxUtils.setStyle(f, mxConstants.STYLE_FONTCOLOR, mxUtils.getValue(p, mxConstants.STYLE_FONTCOLOR, null))));
                                    c.getModel().setStyle(e[m], f)
                                }
                            } finally {
                                c.getModel().endUpdate()
                            }
                        });
                        d.className = "geStyleButton";
                        d.style.width = "36px";
                        d.style.height = 10 >= this.defaultColorSchemes.length ? "24px" : "30px";
                        d.style.margin = "0px 6px 6px 0px";
                        if (null != a) null != a.gradient ? mxClient.IS_IE && (mxClient.IS_QUIRKS || 10 > document.documentMode) ? d.style.filter = "progid:DXImageTransform.Microsoft.Gradient(StartColorStr='" + a.fill + "', EndColorStr='" + a.gradient + "', GradientType=0)" : d.style.backgroundImage = "linear-gradient(" + a.fill + " 0px," + a.gradient + " 100%)" : a.fill == mxConstants.NONE ? d.style.background = "url('" + Dialog.prototype.noColorImage + "')" : d.style.backgroundColor = "" == a.fill ? mxUtils.getValue(b.initialDefaultVertexStyle, mxConstants.STYLE_FILLCOLOR, "dark" == uiTheme ? "#2a2a2a" : "#ffffff") : a.fill || mxUtils.getValue(b.initialDefaultVertexStyle, mxConstants.STYLE_FILLCOLOR, "dark" == uiTheme ? "#2a2a2a" : "#ffffff"), d.style.border = a.stroke == mxConstants.NONE ? "1px solid transparent" : "" == a.stroke ? "1px solid " + mxUtils.getValue(b.initialDefaultVertexStyle, mxConstants.STYLE_STROKECOLOR, "dark" != uiTheme ? "#2a2a2a" : "#ffffff") : "1px solid " + (a.stroke || mxUtils.getValue(b.initialDefaultVertexStyle, mxConstants.STYLE_STROKECOLOR, "dark" != uiTheme ? "#2a2a2a" : "#ffffff"));
                        else {
                            var m = mxUtils.getValue(c.defaultVertexStyle, mxConstants.STYLE_FILLCOLOR, "#ffffff"),
                                f = mxUtils.getValue(c.defaultVertexStyle, mxConstants.STYLE_STROKECOLOR, "#000000");
                            d.style.backgroundColor = m;
                            d.style.border = "1px solid " + f
                        }
                        e.appendChild(d)
                    });
                    e.innerHTML = "";
                    for (var m = 0; m < a.length; m++) 0 < m && 0 == mxUtils.mod(m, 4) && mxUtils.br(e), d(a[m])
                });
            null == this.format.currentScheme ? k("dark" == uiTheme ? 1 : 0) : k(this.format.currentScheme);
            var n = 10 >= this.defaultColorSchemes.length ? 28 : 8,
                t = document.createElement("div");
            t.style.cssText = "position:absolute;left:10px;top:8px;bottom:" + n + "px;width:20px;margin:4px;opacity:0.5;background-repeat:no-repeat;background-position:center center;background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAwAAAAQBAMAAADQT4M0AAAAIVBMVEUAAAB2dnZ4eHh3d3d1dXVxcXF2dnZ2dnZ2dnZxcXF2dnYmb3w1AAAACnRSTlMAfCTkhhvb7cQSPH2JPgAAADRJREFUCNdjwACMAmBKaiGYs2oJmLPKAZ3DabU8AMRTXpUKopislqFyVzCAuUZgikkBZjoAcMYLnp53P/UAAAAASUVORK5CYII=);";
            mxEvent.addListener(t, "click", mxUtils.bind(this, function() {
                k(mxUtils.mod(this.format.currentScheme - 1, this.defaultColorSchemes.length))
            }));
            var z = document.createElement("div");
            z.style.cssText = "position:absolute;left:202px;top:8px;bottom:" + n + "px;width:20px;margin:4px;opacity:0.5;background-repeat:no-repeat;background-position:center center;background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAwAAAAQBAMAAADQT4M0AAAAIVBMVEUAAAB2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnYBuwCcAAAACnRSTlMAfCTkhhvb7cQSPH2JPgAAADZJREFUCNdjQAOMAmBKaiGY8loF5rKswsZlrVo8AUiFrTICcbIWK8A5DF1gDoMymMPApIAwHwCS0Qx/U7qCBQAAAABJRU5ErkJggg==);";
            1 < this.defaultColorSchemes.length && (a.appendChild(t), a.appendChild(z));
            mxEvent.addListener(z, "click", mxUtils.bind(this, function() {
                k(mxUtils.mod(this.format.currentScheme + 1, this.defaultColorSchemes.length))
            }));
            d(t);
            d(z);
            l(this.defaultColorSchemes[this.format.currentScheme]);
            10 >= this.defaultColorSchemes.length && a.appendChild(m);
            return a
        };
        StyleFormatPanel.prototype.addEditOps = function(a) {
            var d = this.format.getSelectionState(),
                b = null;
            1 == this.editorUi.editor.graph.getSelectionCount() && (b = mxUtils.button(mxResources.get("editStyle"), mxUtils.bind(this, function(a) {
                    this.editorUi.actions.get("editStyle").funct()
                })), b.setAttribute("title", mxResources.get("editStyle") + " (" + this.editorUi.actions.get("editStyle").shortcut + ")"),
                b.style.marginBottom = "2px",
                b.style.width = "202px",
                b.style.height = "20px",
                b.style.backgroundColor = "rgb(242 242 242)",
                b.style.borderColor = "rgb(37 37 37)",
                b.style.borderRadius = "3px",
                b.style.borderWidth = "thin",
                a.appendChild(b));
            var c = this.editorUi.editor.graph,
                e = c.view.getState(c.getSelectionCell());
            1 == c.getSelectionCount() && null != e && null != e.shape && null != e.shape.stencil ? (d = mxUtils.button(mxResources.get("editShape"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("editShape").funct()
            })), d.setAttribute("title", mxResources.get("editShape")), d.style.marginBottom = "2px", null == b ? d.style.width = "202px" : (b.style.width = "100px", d.style.width = "100px", d.style.marginLeft = "2px"), a.appendChild(d)) : d.image && (d = mxUtils.button(mxResources.get("editImage"), mxUtils.bind(this, function(a) {
                this.editorUi.actions.get("image").funct()
            })), d.setAttribute("title", mxResources.get("editImage")), d.style.marginBottom = "2px", null == b ? d.style.width = "202px" : (
                b.style.width = "100px",
                d.style.width = "100px",
                d.style.height = "20px",
                d.style.marginLeft = "2px",
                d.style.backgroundColor = "rgb(242 242 242)",
                d.style.borderColor = "rgb(37 37 37)",
                d.style.borderRadius = "3px",
                d.style.borderWidth = "thin"
            ), a.appendChild(d));
            return a
        }
    }
    Graph.prototype.defaultThemeName = "default-style2";
    Graph.prototype.lastPasteXml = null;
    Graph.prototype.pasteCounter = 0;
    Graph.prototype.defaultScrollbars = "0" != urlParams.sb;
    Graph.prototype.defaultPageVisible = "0" != urlParams.pv;
    Graph.prototype.shadowId = "dropShadow";
    Graph.prototype.svgShadowColor = "#3D4574";
    Graph.prototype.svgShadowOpacity = "0.4";
    Graph.prototype.svgShadowBlur = "1.7";
    Graph.prototype.svgShadowSize = "3";
    Graph.prototype.edgeMode = "move" != urlParams.edge;
    var v = Graph.prototype.init;
    Graph.prototype.init = function() {
        function a(a) {
            d = a;
            try {
                if (mxClient.IS_QUIRKS || 7 == document.documentMode || 8 == document.documentMode) d = document.createEventObject(a), d.type = a.type, d.canBubble = a.canBubble, d.cancelable = a.cancelable, d.view = a.view, d.detail = a.detail, d.screenX = a.screenX, d.screenY = a.screenY, d.clientX = a.clientX, d.clientY = a.clientY, d.ctrlKey = a.ctrlKey, d.altKey = a.altKey, d.shiftKey = a.shiftKey, d.metaKey = a.metaKey, d.button = a.button, d.relatedTarget = a.relatedTarget
            } catch (J) {}
        }
        v.apply(this, arguments);
        window.mxFreehand && (this.freehand = new mxFreehand(this));
        var d = null;
        mxEvent.addListener(this.container, "mouseenter", a);
        mxEvent.addListener(this.container, "mousemove", a);
        mxEvent.addListener(this.container, "mouseleave", function(a) {
            d = null
        });
        this.isMouseInsertPoint = function() {
            return null != d
        };
        var b = this.getInsertPoint;
        this.getInsertPoint = function() {
            return null != d ? this.getPointForEvent(d) : b.apply(this, arguments)
        };
        var c = this.layoutManager.getLayout;
        this.layoutManager.getLayout = function(a) {
            var d = this.graph.getCellStyle(a);
            if (null != d) {
                if ("rack" == d.childLayout) {
                    var b = new mxStackLayout(this.graph, !1);
                    b.gridSize = null != d.rackUnitSize ? parseFloat(d.rackUnitSize) : "undefined" !== typeof mxRackContainer ? mxRackContainer.unitSize : 20;
                    b.fill = !0;
                    b.marginLeft = d.marginLeft || 0;
                    b.marginRight = d.marginRight || 0;
                    b.marginTop = d.marginTop || 0;
                    b.marginBottom = d.marginBottom || 0;
                    b.allowGaps = d.allowGaps || 0;
                    b.resizeParent = !1;
                    return b
                }
                if ("undefined" !== typeof mxTableLayout && "tableLayout" == d.childLayout) return b = new mxTableLayout(this.graph), b.rows = d.tableRows || 2, b.columns = d.tableColumns || 2, b.colPercentages = d.colPercentages, b.rowPercentages = d.rowPercentages, b.equalColumns = "1" == mxUtils.getValue(d, "equalColumns", b.colPercentages ? "0" : "1"), b.equalRows = "1" == mxUtils.getValue(d, "equalRows", b.rowPercentages ? "0" : "1"), b.resizeParent = "1" == mxUtils.getValue(d, "resizeParent", "1"), b.border = d.tableBorder || b.border, b.marginLeft = d.marginLeft || 0, b.marginRight = d.marginRight || 0, b.marginTop = d.marginTop || 0, b.marginBottom = d.marginBottom || 0, b.autoAddCol = "1" == mxUtils.getValue(d, "autoAddCol", "0"), b.autoAddRow = "1" == mxUtils.getValue(d, "autoAddRow", b.autoAddCol ? "0" : "1"), b.colWidths = d.colWidths || "100", b.rowHeights = d.rowHeights || "50", b
            }
            return c.apply(this, arguments)
        };
        this.updateGlobalUrlVariables()
    };
    var x = Graph.prototype.isFastZoomEnabled;
    Graph.prototype.isFastZoomEnabled = function() {
        return x.apply(this, arguments) && (!this.shadowVisible || !mxClient.IS_SF)
    };
    Graph.prototype.updateGlobalUrlVariables = function() {
        this.globalVars = Editor.globalVars;
        if (null != urlParams.vars) try {
            this.globalVars = null != this.globalVars ? mxUtils.clone(this.globalVars) : {};
            var a = JSON.parse(decodeURIComponent(urlParams.vars));
            if (null != a)
                for (var d in a) this.globalVars[d] = a[d]
        } catch (G) {
            null != window.console && console.log("Error in vars URL parameter: " + G)
        }
    };
    Graph.prototype.getExportVariables = function() {
        return null != this.globalVars ? mxUtils.clone(this.globalVars) : {}
    };
    var y = Graph.prototype.getGlobalVariable;
    Graph.prototype.getGlobalVariable = function(a) {
        var d = y.apply(this, arguments);
        null == d && null != this.globalVars && (d = this.globalVars[a]);
        return d
    };
    Graph.prototype.getDefaultStylesheet = function() {
        if (null == this.defaultStylesheet) {
            var a = this.themes["default-style2"];
            this.defaultStylesheet = (new mxCodec(a.ownerDocument)).decode(a)
        }
        return this.defaultStylesheet
    };
    Graph.prototype.isViewer = function() {
        return urlParams.viewer
    };
    var A = Graph.prototype.getSvg;
    Graph.prototype.getSvg = function(a, d, b, c, e, g, m, f, n, p, k, l) {
        var t = null;
        l || null == this.themes || "darkTheme" != this.defaultThemeName || (t = this.stylesheet, this.stylesheet = this.getDefaultStylesheet(), this.refresh());
        var z = A.apply(this, arguments);
        if (k && null != this.extFonts && 0 < this.extFonts.length) {
            var u = z.ownerDocument,
                q = null != u.createElementNS ? u.createElementNS(mxConstants.NS_SVG, "style") : u.createElement("style");
            null != u.setAttributeNS ? q.setAttributeNS("type", "text/css") : q.setAttribute("type", "text/css");
            for (var v = "", B = "", x = 0; x < this.extFonts.length; x++) {
                var C = this.extFonts[x].name,
                    y = this.extFonts[x].url;
                0 == y.indexOf(Editor.GOOGLE_FONTS) ? v += "@import url(" + y + ");\n" : B += '@font-face {\nfont-family: "' + C + '";\nsrc: url("' + y + '");\n}\n'
            }
            q.appendChild(u.createTextNode(v + B));
            z.getElementsByTagName("defs")[0].appendChild(q)
        }
        null != t && (this.stylesheet = t, this.refresh());
        return z
    };
    var z = Graph.prototype.createSvgImageExport;
    Graph.prototype.createSvgImageExport = function() {
        var a = z.apply(this, arguments);
        if (this.mathEnabled) {
            this.container.getBoundingClientRect();
            var d = a.drawText;
            a.drawText = function(a, b) {
                if (null != a.text && null != a.text.value && a.text.checkBounds() && (mxUtils.isNode(a.text.value) || a.text.dialect == mxConstants.DIALECT_STRICTHTML)) {
                    var c = a.text.getContentNode();
                    if (null != c) {
                        c = c.cloneNode(!0);
                        if (c.getElementsByTagNameNS)
                            for (var e = c.getElementsByTagNameNS("http://www.w3.org/1998/Math/MathML", "math"); 0 < e.length;) e[0].parentNode.removeChild(e[0]);
                        null != c.innerHTML && (e = a.text.value, a.text.value = c.innerHTML, d.apply(this, arguments), a.text.value = e)
                    }
                } else d.apply(this, arguments)
            }
        }
        return a
    };
    var B = mxGraphView.prototype.validateBackgroundPage;
    mxGraphView.prototype.validateBackgroundPage = function() {
        B.apply(this, arguments);
        if (mxClient.IS_GC && null != this.getDrawPane()) {
            var a = this.getDrawPane().parentNode;
            !this.graph.mathEnabled || mxClient.NO_FO || null != this.webKitForceRepaintNode && null != this.webKitForceRepaintNode.parentNode || "svg" != this.graph.container.firstChild.nodeName ? null == this.webKitForceRepaintNode || this.graph.mathEnabled && ("svg" == this.graph.container.firstChild.nodeName || this.graph.container.firstChild == this.webKitForceRepaintNode) || (null != this.webKitForceRepaintNode.parentNode && this.webKitForceRepaintNode.parentNode.removeChild(this.webKitForceRepaintNode), this.webKitForceRepaintNode = null) : (this.webKitForceRepaintNode = document.createElement("div"), this.webKitForceRepaintNode.style.cssText = "position:absolute;", a.ownerSVGElement.parentNode.insertBefore(this.webKitForceRepaintNode, a.ownerSVGElement))
        }
    };
    var C = Graph.prototype.loadStylesheet;
    Graph.prototype.loadStylesheet = function() {
        C.apply(this, arguments);
        this.currentStyle = "default-style2"
    };
    Graph.prototype.handleCustomLink = function(a) {
        if ("data:action/json," == a.substring(0, 17) && (a = JSON.parse(a.substring(17)), null != a.actions)) {
            for (var d = 0; d < a.actions.length; d++) {
                var b = a.actions[d];
                if (null != b.open)
                    if (this.isCustomLink(b.open)) {
                        if (!this.customLinkClicked(b.open)) return
                    } else this.openLink(b.open)
            }
            this.model.beginUpdate();
            try {
                for (d = 0; d < a.actions.length; d++) b = a.actions[d], null != b.toggle && this.toggleCells(this.getCellsForAction(b.toggle, !0)), null != b.show && this.setCellsVisible(this.getCellsForAction(b.show, !0), !0), null != b.hide && this.setCellsVisible(this.getCellsForAction(b.hide, !0), !1)
            } finally {
                this.model.endUpdate()
            }
            for (d = 0; d < a.actions.length; d++) {
                var b = a.actions[d],
                    c = [];
                null != b.select && this.isEnabled() && (c = this.getCellsForAction(b.select), this.setSelectionCells(c));
                null != b.highlight && (c = this.getCellsForAction(b.highlight), this.highlightCells(c, b.highlight.color, b.highlight.duration, b.highlight.opacity));
                null != b.scroll && (c = this.getCellsForAction(b.scroll));
                null != b.viewbox && this.fitWindow(b.viewbox, b.viewbox.border);
                0 < c.length && this.scrollCellToVisible(c[0])
            }
        }
    };
    Graph.prototype.updateCustomLinksForCell = function(a, d) {
        var b = this.getLinkForCell(d);
        null != b && "data:action/json," == b.substring(0, 17) && this.setLinkForCell(d, this.updateCustomLink(a, b));
        if (this.isHtmlLabel(d)) {
            var c = document.createElement("div");
            c.innerHTML = this.sanitizeHtml(this.getLabel(d));
            for (var e = c.getElementsByTagName("a"), g = !1, m = 0; m < e.length; m++) b = e[m].getAttribute("href"), null != b && "data:action/json," == b.substring(0, 17) && (e[m].setAttribute("href", this.updateCustomLink(a, b)), g = !0);
            g && this.labelChanged(d, c.innerHTML)
        }
    };
    Graph.prototype.updateCustomLink = function(a, d) {
        if ("data:action/json," == d.substring(0, 17)) try {
            var b = JSON.parse(d.substring(17));
            null != b.actions && (this.updateCustomLinkActions(a, b.actions), d = "data:action/json," + JSON.stringify(b))
        } catch (L) {}
        return d
    };
    Graph.prototype.updateCustomLinkActions = function(a, d) {
        for (var b = 0; b < d.length; b++) {
            var c = d[b];
            this.updateCustomLinkAction(a, c.toggle);
            this.updateCustomLinkAction(a, c.show);
            this.updateCustomLinkAction(a, c.hide);
            this.updateCustomLinkAction(a, c.select);
            this.updateCustomLinkAction(a, c.highlight);
            this.updateCustomLinkAction(a, c.scroll)
        }
    };
    Graph.prototype.updateCustomLinkAction = function(a, d) {
        if (null != d && null != d.cells) {
            for (var b = [], c = 0; c < d.cells.length; c++)
                if ("*" == d.cells[c]) b.push(d.cells[c]);
                else {
                    var e = a[d.cells[c]];
                    null != e ? "" != e && b.push(e) : b.push(d.cells[c])
                }
            d.cells = b
        }
    };
    Graph.prototype.getCellsForAction = function(a, d) {
        return this.getCellsById(a.cells).concat(this.getCellsForTags(a.tags, null, null, d))
    };
    Graph.prototype.getCellsById = function(a) {
        var d = [];
        if (null != a)
            for (var b = 0; b < a.length; b++)
                if ("*" == a[b]) var c = this.getDefaultParent(),
                    d = d.concat(this.model.filterDescendants(function(a) {
                        return a != c
                    }, c));
                else {
                    var e = this.model.getCell(a[b]);
                    null != e && d.push(e)
                }
        return d
    };
    Graph.prototype.getCellsForTags = function(a, d, b, c) {
        var e = [];
        if (null != a) {
            d = null != d ? d : this.model.getDescendants(this.model.getRoot());
            b = null != b ? b : "tags";
            for (var g = 0, m = {}, f = 0; f < a.length; f++) 0 < a[f].length && (m[a[f].toLowerCase()] = !0, g++);
            for (f = 0; f < d.length; f++)
                if (c && this.model.getParent(d[f]) == this.model.root || this.model.isVertex(d[f]) || this.model.isEdge(d[f])) {
                    var n = null != d[f].value && "object" == typeof d[f].value ? mxUtils.trim(d[f].value.getAttribute(b) || "") : "",
                        p = !1;
                    if (0 < n.length) {
                        if (n = n.toLowerCase().split(" "), n.length >= a.length) {
                            for (var k = p = 0; k < n.length && p < g; k++) null != m[n[k]] && p++;
                            p = p == g
                        }
                    } else p = 0 == a.length;
                    p && e.push(d[f])
                }
        }
        return e
    };
    Graph.prototype.toggleCells = function(a) {
        this.model.beginUpdate();
        try {
            for (var d = 0; d < a.length; d++) this.model.setVisible(a[d], !this.model.isVisible(a[d]))
        } finally {
            this.model.endUpdate()
        }
    };
    Graph.prototype.setCellsVisible = function(a, d) {
        this.model.beginUpdate();
        try {
            for (var b = 0; b < a.length; b++) this.model.setVisible(a[b], d)
        } finally {
            this.model.endUpdate()
        }
    };
    Graph.prototype.highlightCells = function(a, d, b, c) {
        for (var e = 0; e < a.length; e++) this.highlightCell(a[e], d, b, c)
    };
    Graph.prototype.highlightCell = function(a, d, b, c) {
        d = null != d ? d : mxConstants.DEFAULT_VALID_COLOR;
        b = null != b ? b : 1E3;
        a = this.view.getState(a);
        if (null != a) {
            var e = Math.max(5, mxUtils.getValue(a.style, mxConstants.STYLE_STROKEWIDTH, 1) + 4),
                g = new mxCellHighlight(this, d, e, !1);
            null != c && (g.opacity = c);
            g.highlight(a);
            window.setTimeout(function() {
                null != g.shape && (mxUtils.setPrefixedStyle(g.shape.node.style, "transition", "all 1200ms ease-in-out"), g.shape.node.style.opacity = 0);
                window.setTimeout(function() {
                    g.destroy()
                }, 1200)
            }, b)
        }
    };
    Graph.prototype.addSvgShadow = function(a, d, b) {
        b = null != b ? b : !1;
        var c = a.ownerDocument,
            e = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "filter") : c.createElement("filter");
        e.setAttribute("id", this.shadowId);
        var g = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "feGaussianBlur") : c.createElement("feGaussianBlur");
        g.setAttribute("in", "SourceAlpha");
        g.setAttribute("stdDeviation", this.svgShadowBlur);
        g.setAttribute("result", "blur");
        e.appendChild(g);
        g = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "feOffset") : c.createElement("feOffset");
        g.setAttribute("in", "blur");
        g.setAttribute("dx", this.svgShadowSize);
        g.setAttribute("dy", this.svgShadowSize);
        g.setAttribute("result", "offsetBlur");
        e.appendChild(g);
        g = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "feFlood") : c.createElement("feFlood");
        g.setAttribute("flood-color", this.svgShadowColor);
        g.setAttribute("flood-opacity", this.svgShadowOpacity);
        g.setAttribute("result", "offsetColor");
        e.appendChild(g);
        g = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "feComposite") : c.createElement("feComposite");
        g.setAttribute("in", "offsetColor");
        g.setAttribute("in2", "offsetBlur");
        g.setAttribute("operator", "in");
        g.setAttribute("result", "offsetBlur");
        e.appendChild(g);
        g = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "feBlend") : c.createElement("feBlend");
        g.setAttribute("in", "SourceGraphic");
        g.setAttribute("in2", "offsetBlur");
        e.appendChild(g);
        g = a.getElementsByTagName("defs");
        0 == g.length ? (c = null != c.createElementNS ? c.createElementNS(mxConstants.NS_SVG, "defs") : c.createElement("defs"), null != a.firstChild ? a.insertBefore(c, a.firstChild) : a.appendChild(c)) : c = g[0];
        c.appendChild(e);
        b || (d = null != d ? d : a.getElementsByTagName("g")[0], null != d && (d.setAttribute("filter", "url(#" + this.shadowId + ")"), isNaN(parseInt(a.getAttribute("width"))) || (a.setAttribute("width", parseInt(a.getAttribute("width")) + 6), a.setAttribute("height", parseInt(a.getAttribute("height")) + 6), d = a.getAttribute("viewBox"), null != d && 0 < d.length && (d = d.split(" "), 3 < d.length && (w = parseFloat(d[2]) + 6, h = parseFloat(d[3]) + 6, a.setAttribute("viewBox", d[0] + " " + d[1] + " " + w + " " + h))))));
        return e
    };
    Graph.prototype.setShadowVisible = function(a, d) {
        mxClient.IS_SVG && !mxClient.IS_SF && (d = null != d ? d : !0, (this.shadowVisible = a) ? this.view.getDrawPane().setAttribute("filter", "url(#" + this.shadowId + ")") : this.view.getDrawPane().removeAttribute("filter"), d && this.fireEvent(new mxEventObject("shadowVisibleChanged")))
    };
    Graph.prototype.selectUnlockedLayer = function() {
        if (null == this.defaultParent) {
            var a = this.model.getChildCount(this.model.root),
                d, b = 0;
            do d = this.model.getChildAt(this.model.root, b); while (b++ < a && "1" == mxUtils.getValue(this.getCellStyle(d), "locked", "0"));
            null != d && this.setDefaultParent(d)
        }
    };
    mxStencilRegistry.libraries.mockup = [SHAPES_PATH + "/mockup/mxMockupButtons.js"];
    mxStencilRegistry.libraries.arrows2 = [SHAPES_PATH + "/mxArrows.js"];
    mxStencilRegistry.libraries.atlassian = [STENCIL_PATH + "/atlassian.xml", SHAPES_PATH + "/mxAtlassian.js"];
    mxStencilRegistry.libraries.bpmn = [SHAPES_PATH + "/bpmn/mxBpmnShape2.js", STENCIL_PATH + "/bpmn.xml"];
    mxStencilRegistry.libraries.c4 = [SHAPES_PATH + "/mxC4.js"];
    mxStencilRegistry.libraries.cisco19 = [SHAPES_PATH + "/mxCisco19.js", STENCIL_PATH + "/cisco19.xml"];
    mxStencilRegistry.libraries.cisco_safe = [SHAPES_PATH + "/mxCiscoSafe.js", STENCIL_PATH + "/cisco_safe/architecture.xml", STENCIL_PATH + "/cisco_safe/business_icons.xml", STENCIL_PATH + "/cisco_safe/capability.xml", STENCIL_PATH + "/cisco_safe/design.xml",
        STENCIL_PATH + "/cisco_safe/iot_things_icons.xml", STENCIL_PATH + "/cisco_safe/people_places_things_icons.xml", STENCIL_PATH + "/cisco_safe/security_icons.xml", STENCIL_PATH + "/cisco_safe/technology_icons.xml", STENCIL_PATH + "/cisco_safe/threat.xml"
    ];
    mxStencilRegistry.libraries.dfd = [SHAPES_PATH + "/mxDFD.js"];
    mxStencilRegistry.libraries.er = [SHAPES_PATH + "/er/mxER.js"];
    mxStencilRegistry.libraries.kubernetes = [SHAPES_PATH + "/mxKubernetes.js", STENCIL_PATH + "/kubernetes.xml"];
    mxStencilRegistry.libraries.flowchart = [SHAPES_PATH + "/mxFlowchart.js", STENCIL_PATH + "/flowchart.xml"];
    mxStencilRegistry.libraries.ios = [SHAPES_PATH + "/mockup/mxMockupiOS.js"];
    mxStencilRegistry.libraries.rackGeneral = [SHAPES_PATH + "/rack/mxRack.js", STENCIL_PATH + "/rack/general.xml"];
    mxStencilRegistry.libraries.rackF5 = [STENCIL_PATH + "/rack/f5.xml"];
    mxStencilRegistry.libraries.lean_mapping = [SHAPES_PATH + "/mxLeanMap.js", STENCIL_PATH + "/lean_mapping.xml"];
    mxStencilRegistry.libraries.basic = [SHAPES_PATH + "/mxBasic.js", STENCIL_PATH + "/basic.xml"];
    mxStencilRegistry.libraries.ios7icons = [STENCIL_PATH + "/ios7/icons.xml"];
    mxStencilRegistry.libraries.ios7ui = [SHAPES_PATH + "/ios7/mxIOS7Ui.js", STENCIL_PATH + "/ios7/misc.xml"];
    mxStencilRegistry.libraries.android = [SHAPES_PATH + "/mxAndroid.js", STENCIL_PATH + "/android/android.xml"];
    mxStencilRegistry.libraries["electrical/miscellaneous"] = [SHAPES_PATH + "/mxElectrical.js", STENCIL_PATH + "/electrical/miscellaneous.xml"];
    mxStencilRegistry.libraries["electrical/transmission"] = [SHAPES_PATH + "/mxElectrical.js", STENCIL_PATH + "/electrical/transmission.xml"];
    mxStencilRegistry.libraries["electrical/logic_gates"] = [SHAPES_PATH + "/mxElectrical.js", STENCIL_PATH + "/electrical/logic_gates.xml"];
    mxStencilRegistry.libraries["electrical/abstract"] = [SHAPES_PATH + "/mxElectrical.js", STENCIL_PATH + "/electrical/abstract.xml"];
    mxStencilRegistry.libraries.infographic = [SHAPES_PATH + "/mxInfographic.js"];
    mxStencilRegistry.libraries["mockup/buttons"] = [SHAPES_PATH + "/mockup/mxMockupButtons.js"];
    mxStencilRegistry.libraries["mockup/containers"] = [SHAPES_PATH + "/mockup/mxMockupContainers.js"];
    mxStencilRegistry.libraries["mockup/forms"] = [SHAPES_PATH + "/mockup/mxMockupForms.js"];
    mxStencilRegistry.libraries["mockup/graphics"] = [SHAPES_PATH + "/mockup/mxMockupGraphics.js", STENCIL_PATH + "/mockup/misc.xml"];
    mxStencilRegistry.libraries["mockup/markup"] = [SHAPES_PATH + "/mockup/mxMockupMarkup.js"];
    mxStencilRegistry.libraries["mockup/misc"] = [SHAPES_PATH + "/mockup/mxMockupMisc.js", STENCIL_PATH + "/mockup/misc.xml"];
    mxStencilRegistry.libraries["mockup/navigation"] = [SHAPES_PATH + "/mockup/mxMockupNavigation.js", STENCIL_PATH + "/mockup/misc.xml"];
    mxStencilRegistry.libraries["mockup/text"] = [SHAPES_PATH + "/mockup/mxMockupText.js"];
    mxStencilRegistry.libraries.floorplan = [SHAPES_PATH + "/mxFloorplan.js", STENCIL_PATH + "/floorplan.xml"];
    mxStencilRegistry.libraries.bootstrap = [SHAPES_PATH + "/mxBootstrap.js", SHAPES_PATH + "/mxBasic.js", STENCIL_PATH + "/bootstrap.xml"];
    mxStencilRegistry.libraries.gmdl = [SHAPES_PATH + "/mxGmdl.js", STENCIL_PATH + "/gmdl.xml"];
    mxStencilRegistry.libraries.gcp2 = [SHAPES_PATH + "/mxGCP2.js", STENCIL_PATH + "/gcp2.xml"];
    mxStencilRegistry.libraries.ibm = [SHAPES_PATH + "/mxIBM.js", STENCIL_PATH + "/ibm.xml"];
    mxStencilRegistry.libraries.cabinets = [SHAPES_PATH + "/mxCabinets.js", STENCIL_PATH + "/cabinets.xml"];
    mxStencilRegistry.libraries.archimate = [SHAPES_PATH + "/mxArchiMate.js"];
    mxStencilRegistry.libraries.archimate3 = [SHAPES_PATH + "/mxArchiMate3.js"];
    mxStencilRegistry.libraries.sysml = [SHAPES_PATH + "/mxSysML.js"];
    mxStencilRegistry.libraries.eip = [SHAPES_PATH + "/mxEip.js", STENCIL_PATH + "/eip.xml"];
    mxStencilRegistry.libraries.networks = [SHAPES_PATH + "/mxNetworks.js", STENCIL_PATH + "/networks.xml"];
    mxStencilRegistry.libraries.aws3d = [SHAPES_PATH + "/mxAWS3D.js", STENCIL_PATH + "/aws3d.xml"];
    mxStencilRegistry.libraries.aws4 = [SHAPES_PATH + "/mxAWS4.js", STENCIL_PATH + "/aws4.xml"];
    mxStencilRegistry.libraries.aws4b = [SHAPES_PATH + "/mxAWS4.js", STENCIL_PATH + "/aws4.xml"];
    mxStencilRegistry.libraries.veeam = [STENCIL_PATH + "/veeam/2d.xml", STENCIL_PATH + "/veeam/3d.xml", STENCIL_PATH + "/veeam/veeam.xml"];
    mxStencilRegistry.libraries.veeam2 = [STENCIL_PATH + "/veeam/2d.xml", STENCIL_PATH + "/veeam/3d.xml", STENCIL_PATH + "/veeam/veeam2.xml"];
    mxStencilRegistry.libraries.pid2inst = [SHAPES_PATH + "/pid2/mxPidInstruments.js"];
    mxStencilRegistry.libraries.pid2misc = [SHAPES_PATH + "/pid2/mxPidMisc.js", STENCIL_PATH + "/pid/misc.xml"];
    mxStencilRegistry.libraries.pid2valves = [SHAPES_PATH + "/pid2/mxPidValves.js"];
    mxStencilRegistry.libraries.pidFlowSensors = [STENCIL_PATH + "/pid/flow_sensors.xml"];
    mxMarker.getPackageForType = function(a) {
        var d = null;
        null != a && 0 < a.length && ("ER" == a.substring(0, 2) ? d = "mxgraph.er" : "sysML" == a.substring(0, 5) && (d = "mxgraph.sysml"));
        return d
    };
    var D = mxMarker.createMarker;
    mxMarker.createMarker = function(a, d, b, c, e, g, m, f, n, p) {
        if (null != b && null == mxMarker.markers[b]) {
            var k = this.getPackageForType(b);
            null != k && mxStencilRegistry.getStencil(k)
        }
        return D.apply(this, arguments)
    };
    PrintDialog.prototype.create = function(a, d) {
        function b() {
            z.value = Math.max(1, Math.min(f, Math.max(parseInt(z.value), parseInt(t.value))));
            t.value = Math.max(1, Math.min(f, Math.min(parseInt(z.value), parseInt(t.value))))
        }

        function c(d) {
            function b(d, b, g) {
                var m = d.useCssTransforms,
                    f = d.currentTranslate,
                    n = d.currentScale,
                    p = d.view.translate,
                    k = d.view.scale;
                d.useCssTransforms && (d.useCssTransforms = !1, d.currentTranslate = new mxPoint(0, 0), d.currentScale = 1, d.view.translate = new mxPoint(0, 0), d.view.scale = 1);
                var l = d.getGraphBounds(),
                    t = 0,
                    z = 0,
                    u = oa.get(),
                    q = 1 / d.pageScale,
                    x = B.checked;
                if (x) var q = parseInt(K.value),
                    C = parseInt(ja.value),
                    q = Math.min(u.height * C / (l.height / d.view.scale), u.width * q / (l.width / d.view.scale));
                else q = parseInt(v.value) / (100 * d.pageScale), isNaN(q) && (c = 1 / d.pageScale, v.value = "100 %");
                u = mxRectangle.fromRectangle(u);
                u.width = Math.ceil(u.width * c);
                u.height = Math.ceil(u.height * c);
                q *= c;
                !x && d.pageVisible ? (l = d.getPageLayout(), t -= l.x * u.width, z -= l.y * u.height) : x = !0;
                if (null == b) {
                    b = PrintDialog.createPrintPreview(d, q, u, 0, t, z, x);
                    b.pageSelector = !1;
                    b.mathEnabled = !1;
                    t = a.getCurrentFile();
                    null != t && (b.title = t.getTitle());
                    var y = b.writeHead;
                    b.writeHead = function(b) {
                        y.apply(this, arguments);
                        mxClient.IS_GC && (b.writeln('<style type="text/css">'), b.writeln("@media print {"), b.writeln("span.MathJax_SVG svg { shape-rendering: crispEdges; }"), b.writeln("}"), b.writeln("</style>"));
                        null != a.editor.fontCss && (b.writeln('<style type="text/css">'), b.writeln(a.editor.fontCss), b.writeln("</style>"));
                        if (null != d.extFonts)
                            for (var c = 0; c < d.extFonts.length; c++) {
                                var e = d.extFonts[c].name,
                                    g = d.extFonts[c].url;
                                0 == g.indexOf(Editor.GOOGLE_FONTS) ? b.writeln('<link rel="stylesheet" href="' + g + '" charset="UTF-8" type="text/css">') : (b.writeln('<style type="text/css">'), b.writeln('@font-face {\n\tfont-family: "' + e + '";\n\tsrc: url("' + g + '");\n}'), b.writeln("</style>"))
                            }
                    };
                    if ("undefined" !== typeof MathJax) {
                        var D = b.renderPage;
                        b.renderPage = function(d, b, c, e, g, m) {
                            var f = mxClient.NO_FO;
                            mxClient.NO_FO = this.graph.mathEnabled && !a.editor.useForeignObjectForMath ? !0 : a.editor.originalNoForeignObject;
                            var n = D.apply(this, arguments);
                            mxClient.NO_FO = f;
                            this.graph.mathEnabled ? this.mathEnabled = this.mathEnabled || !0 : n.className = "geDisableMathJax";
                            return n
                        }
                    }
                    t = null;
                    null != e.themes && "darkTheme" == e.defaultThemeName && (t = e.stylesheet, e.stylesheet = e.getDefaultStylesheet(), e.refresh());
                    b.open(null, null, g, !0);
                    null != t && (e.stylesheet = t, e.refresh())
                } else {
                    u = d.background;
                    if (null == u || "" == u || u == mxConstants.NONE) u = "#ffffff";
                    b.backgroundColor = u;
                    b.autoOrigin = x;
                    b.appendGraph(d, q, t, z, g, !0);
                    if (null != d.extFonts && null != b.wnd)
                        for (g = 0; g < d.extFonts.length; g++) t = d.extFonts[g].name, z = d.extFonts[g].url, 0 == z.indexOf(Editor.GOOGLE_FONTS) ? b.wnd.document.writeln('<link rel="stylesheet" href="' + z + '" charset="UTF-8" type="text/css">') : (b.wnd.document.writeln('<style type="text/css">'), b.wnd.document.writeln('@font-face {\n\tfont-family: "' + t + '";\n\tsrc: url("' + z + '");\n}'), b.wnd.document.writeln("</style>"))
                }
                m && (d.useCssTransforms = m, d.currentTranslate = f, d.currentScale = n, d.view.translate = p, d.view.scale = k);
                return b
            }
            var c = parseInt(ha.value) / 100;
            isNaN(c) && (c = 1, ha.value = "100 %");
            var c = .75 * c,
                g = null;
            null != e.themes && "darkTheme" == e.defaultThemeName && (g = e.stylesheet, e.stylesheet = e.getDefaultStylesheet(), e.refresh());
            var m = t.value,
                f = z.value,
                p = !k.checked,
                l = null;
            if (EditorUi.isElectronApp) PrintDialog.electronPrint(a, k.checked, m, f, B.checked, K.value, ja.value, parseInt(v.value) / 100, parseInt(ha.value) / 100, oa.get());
            else {
                p && (p = m == n && f == n);
                if (!p && null != a.pages && a.pages.length) {
                    var u = 0,
                        p = a.pages.length - 1;
                    k.checked || (u = parseInt(m) - 1, p = parseInt(f) - 1);
                    for (var q = u; q <= p; q++) {
                        var x = a.pages[q],
                            m = x == a.currentPage ? e : null;
                        if (null == m) {
                            var m = a.createTemporaryGraph(e.stylesheet),
                                f = !0,
                                u = !1,
                                C = null,
                                y = null;
                            null == x.viewState && null == x.root && a.updatePageRoot(x);
                            null != x.viewState && (f = x.viewState.pageVisible, u = x.viewState.mathEnabled, C = x.viewState.background, y = x.viewState.backgroundImage, m.extFonts = x.viewState.extFonts);
                            m.background = C;
                            m.backgroundImage = null != y ? new mxImage(y.src, y.width, y.height) : null;
                            m.pageVisible = f;
                            m.mathEnabled = u;
                            var D = m.getGlobalVariable;
                            m.getGlobalVariable = function(d) {
                                return "page" == d ? x.getName() : "pagenumber" == d ? q + 1 : "pagecount" == d ? null != a.pages ? a.pages.length : 1 : D.apply(this, arguments)
                            };
                            document.body.appendChild(m.container);
                            a.updatePageRoot(x);
                            m.model.setRoot(x.root)
                        }
                        l = b(m, l, q != p);
                        m != e && m.container.parentNode.removeChild(m.container)
                    }
                } else l = b(e);
                null == l ? a.handleError({
                    message: mxResources.get("errorUpdatingPreview")
                }) : (l.mathEnabled && (p = l.wnd.document, d && (l.wnd.IMMEDIATE_PRINT = !0), p.writeln('<script type="text/javascript" src="' + DRAWIO_BASE_URL + '/js/math-print.js"></script>')), l.closeDocument(), !l.mathEnabled && d && PrintDialog.printPreview(l));
                null != g && (e.stylesheet = g, e.refresh())
            }
        }
        var e = a.editor.graph,
            g = document.createElement("div"),
            m = document.createElement("h3");
        m.style.width = "100%";
        m.style.textAlign = "center";
        m.style.marginTop = "0px";
        mxUtils.write(m, d || mxResources.get("print"));
        g.appendChild(m);
        var f = 1,
            n = 1,
            p = document.createElement("div");
        p.style.cssText = "border-bottom:1px solid lightGray;padding-bottom:12px;margin-bottom:12px;";
        var k = document.createElement("input");
        k.style.cssText = "margin-right:8px;margin-bottom:8px;";
        k.setAttribute("value", "all");
        k.setAttribute("type", "radio");
        k.setAttribute("name", "pages-printdialog");
        p.appendChild(k);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("printAllPages"));
        p.appendChild(m);
        mxUtils.br(p);
        var l = k.cloneNode(!0);
        k.setAttribute("checked", "checked");
        l.setAttribute("value", "range");
        p.appendChild(l);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("pages") + ":");
        p.appendChild(m);
        var t = document.createElement("input");
        t.style.cssText = "margin:0 8px 0 8px;";
        t.setAttribute("value", "1");
        t.setAttribute("type", "number");
        t.setAttribute("min", "1");
        t.style.width = "50px";
        p.appendChild(t);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("to"));
        p.appendChild(m);
        var z = t.cloneNode(!0);
        p.appendChild(z);
        mxEvent.addListener(t, "focus", function() {
            l.checked = !0
        });
        mxEvent.addListener(z, "focus", function() {
            l.checked = !0
        });
        mxEvent.addListener(t, "change", b);
        mxEvent.addListener(z, "change", b);
        if (null != a.pages && (f = a.pages.length, null != a.currentPage))
            for (m = 0; m < a.pages.length; m++)
                if (a.currentPage == a.pages[m]) {
                    n = m + 1;
                    t.value = n;
                    z.value = n;
                    break
                }
        t.setAttribute("max", f);
        z.setAttribute("max", f);
        a.isPagesEnabled() ? 1 < f && (g.appendChild(p), l.checked = !0) : l.checked = !0;
        var u = document.createElement("div");
        u.style.marginBottom = "10px";
        var q = document.createElement("input");
        q.style.marginRight = "8px";
        q.setAttribute("value", "adjust");
        q.setAttribute("type", "radio");
        q.setAttribute("name", "printZoom");
        u.appendChild(q);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("adjustTo"));
        u.appendChild(m);
        var v = document.createElement("input");
        v.style.cssText = "margin:0 8px 0 8px;";
        v.setAttribute("value", "100 %");
        v.style.width = "50px";
        u.appendChild(v);
        mxEvent.addListener(v, "focus", function() {
            q.checked = !0
        });
        g.appendChild(u);
        var p = p.cloneNode(!1),
            B = q.cloneNode(!0);
        B.setAttribute("value", "fit");
        q.setAttribute("checked", "checked");
        m = document.createElement("div");
        m.style.cssText = "display:inline-block;height:100%;vertical-align:top;padding-top:2px;";
        m.appendChild(B);
        p.appendChild(m);
        u = document.createElement("table");
        u.style.display = "inline-block";
        var x = document.createElement("tbody"),
            C = document.createElement("tr"),
            y = C.cloneNode(!0),
            D = document.createElement("td"),
            A = D.cloneNode(!0),
            I = D.cloneNode(!0),
            E = D.cloneNode(!0),
            F = D.cloneNode(!0),
            H = D.cloneNode(!0);
        D.style.textAlign = "right";
        E.style.textAlign = "right";
        mxUtils.write(D, mxResources.get("fitTo"));
        var K = document.createElement("input");
        K.style.cssText = "margin:0 8px 0 8px;";
        K.setAttribute("value", "1");
        K.setAttribute("min", "1");
        K.setAttribute("type", "number");
        K.style.width = "40px";
        A.appendChild(K);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("fitToSheetsAcross"));
        I.appendChild(m);
        mxUtils.write(E, mxResources.get("fitToBy"));
        var ja = K.cloneNode(!0);
        F.appendChild(ja);
        mxEvent.addListener(K, "focus", function() {
            B.checked = !0
        });
        mxEvent.addListener(ja, "focus", function() {
            B.checked = !0
        });
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("fitToSheetsDown"));
        H.appendChild(m);
        C.appendChild(D);
        C.appendChild(A);
        C.appendChild(I);
        y.appendChild(E);
        y.appendChild(F);
        y.appendChild(H);
        x.appendChild(C);
        x.appendChild(y);
        u.appendChild(x);
        p.appendChild(u);
        g.appendChild(p);
        p = document.createElement("div");
        m = document.createElement("div");
        m.style.fontWeight = "bold";
        m.style.marginBottom = "12px";
        mxUtils.write(m, mxResources.get("paperSize"));
        p.appendChild(m);
        m = document.createElement("div");
        m.style.marginBottom = "12px";
        var oa = PageSetupDialog.addPageFormatPanel(m, "printdialog", a.editor.graph.pageFormat || mxConstants.PAGE_FORMAT_A4_PORTRAIT);
        p.appendChild(m);
        m = document.createElement("span");
        mxUtils.write(m, mxResources.get("pageScale"));
        p.appendChild(m);
        var ha = document.createElement("input");
        ha.style.cssText = "margin:0 8px 0 8px;";
        ha.setAttribute("value", "100 %");
        ha.style.width = "60px";
        p.appendChild(ha);
        g.appendChild(p);
        m = document.createElement("div");
        m.style.cssText = "text-align:right;margin:48px 0 0 0;";
        p = mxUtils.button(mxResources.get("cancel"), function() {
            a.hideDialog()
        });
        p.className = "geBtn";
        a.editor.cancelFirst && m.appendChild(p);
        a.isOffline() || (u = mxUtils.button(mxResources.get("help"), function() {
            e.openLink("https://desk.draw.io/support/solutions/articles/16000048947")
        }), u.className = "geBtn", m.appendChild(u));
        PrintDialog.previewEnabled && (u = mxUtils.button(mxResources.get("preview"), function() {
            a.hideDialog();
            c(!1)
        }), u.className = "geBtn", m.appendChild(u));
        u = mxUtils.button(mxResources.get(PrintDialog.previewEnabled ? "print" : "ok"), function() {
            a.hideDialog();
            c(!0)
        });
        u.className = "geBtn gePrimaryBtn";
        m.appendChild(u);
        a.editor.cancelFirst || m.appendChild(p);
        g.appendChild(m);
        this.container = g
    };
    var E = ChangePageSetup.prototype.execute;
    ChangePageSetup.prototype.execute = function() {
        null == this.page && (this.page = this.ui.currentPage);
        this.page != this.ui.currentPage ? null != this.page.viewState && (this.ignoreColor || (this.page.viewState.background = this.color), this.ignoreImage || (this.page.viewState.backgroundImage = this.image), null != this.format && (this.page.viewState.pageFormat = this.format), null != this.mathEnabled && (this.page.viewState.mathEnabled = this.mathEnabled), null != this.shadowVisible && (this.page.viewState.shadowVisible = this.shadowVisible)) : (E.apply(this, arguments), null != this.mathEnabled && this.mathEnabled != this.ui.isMathEnabled() && (this.ui.setMathEnabled(this.mathEnabled), this.mathEnabled = !this.mathEnabled), null != this.shadowVisible && this.shadowVisible != this.ui.editor.graph.shadowVisible && (this.ui.editor.graph.setShadowVisible(this.shadowVisible), this.shadowVisible = !this.shadowVisible))
    };
    Editor.prototype.useCanvasForExport = !1;
    try {
        var F = document.createElement("canvas"),
            H = new Image;
        H.onload = function() {
            try {
                F.getContext("2d").drawImage(H, 0, 0);
                var a = F.toDataURL("image/png");
                Editor.prototype.useCanvasForExport = null != a && 6 < a.length
            } catch (K) {}
        };
        H.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1px" height="1px" version="1.1"><foreignObject pointer-events="all" width="1" height="1"><div xmlns="http://www.w3.org/1999/xhtml"></div></foreignObject></svg>')))
    } catch (I) {}
})();

(function() {
    EditorUi.VERSION = "13.6.2";
    EditorUi.compactUi = "atlas" != uiTheme;
    mxGraphView.prototype.defaultDarkGridColor = "#6e6e6e";
    "dark" == uiTheme && (mxGraphView.prototype.gridColor = mxGraphView.prototype.defaultDarkGridColor);
    EditorUi.enableLogging = "1" != urlParams.stealth && (/.*\.draw\.io$/.test(window.location.hostname) || /.*\.diagrams\.net$/.test(window.location.hostname)) && "support.draw.io" != window.location.hostname;
    EditorUi.drawHost = window.DRAWIO_BASE_URL;
    EditorUi.lightboxHost = window.DRAWIO_LIGHTBOX_URL;
    EditorUi.lastErrorMessage = null;
    EditorUi.ignoredAnonymizedChars = "\n\t`~!@#$%^&*()_+{}|:\"<>?-=[];'./,\n\t";
    EditorUi.templateFile = TEMPLATE_PATH + "/index.xml";
    EditorUi.cacheUrl = "1" == urlParams.dev ? "/cache" : window.REALTIME_URL;
    null == EditorUi.cacheUrl && "undefined" !== typeof DrawioFile && (DrawioFile.SYNC = "none");
    Editor.cacheTimeout = 1E4;
    EditorUi.enablePlantUml = EditorUi.enableLogging;
    EditorUi.isElectronApp = null != window && null != window.process && null != window.process.versions && null != window.process.versions.electron;
    EditorUi.enableDrafts = !mxClient.IS_CHROMEAPP && !EditorUi.isElectronApp && isLocalStorage && "0" != urlParams.drafts;
    EditorUi.scratchpadHelpLink = "https://desk.draw.io/support/solutions/articles/16000042367";
    EditorUi.defaultMermaidConfig = {
        theme: "neutral",
        arrowMarkerAbsolute: !1,
        flowchart: {
            htmlLabels: !1
        },
        sequence: {
            diagramMarginX: 50,
            diagramMarginY: 10,
            actorMargin: 50,
            width: 150,
            height: 65,
            boxMargin: 10,
            boxTextMargin: 5,
            noteMargin: 10,
            messageMargin: 35,
            mirrorActors: !0,
            bottomMarginAdj: 1,
            useMaxWidth: !0,
            rightAngles: !1,
            showSequenceNumbers: !1
        },
        gantt: {
            titleTopMargin: 25,
            barHeight: 20,
            barGap: 4,
            topPadding: 50,
            leftPadding: 75,
            gridLineStartPadding: 35,
            fontSize: 14,
            fontFamily: '"Open-Sans", "sans-serif"',
            numberSectionStyles: 4,
            axisFormat: "%Y-%m-%d"
        }
    };
    EditorUi.logError = function(a, b, c, e, f, k, l) {
        k = null != k ? k : 0 <= a.indexOf("NetworkError") || 0 <= a.indexOf("SecurityError") || 0 <= a.indexOf("NS_ERROR_FAILURE") || 0 <= a.indexOf("out of memory") ? "CONFIG" : "SEVERE";
        if (EditorUi.enableLogging && "1" != urlParams.dev) try {
            if (a != EditorUi.lastErrorMessage && (null == a || null == b || -1 == a.indexOf("Script error") && -1 == a.indexOf("extension")) && null != a && 0 > a.indexOf("DocumentClosedError")) {
                EditorUi.lastErrorMessage = a;
                var d = null != window.DRAWIO_LOG_URL ? window.DRAWIO_LOG_URL : "";
                f = null != f ? f : Error(a);
                (new Image).src = d + "/log?severity=" + k + "&v=" + encodeURIComponent(EditorUi.VERSION) + "&msg=clientError:" + encodeURIComponent(a) + ":url:" + encodeURIComponent(window.location.href) + ":lnum:" + encodeURIComponent(c) + (null != e ? ":colno:" + encodeURIComponent(e) : "") + (null != f && null != f.stack ? "&stack=" + encodeURIComponent(f.stack) : "")
            }
        } catch (v) {}
        try {
            l || null == window.console || console.error(k, a, b, c, e, f)
        } catch (v) {}
    };
    EditorUi.logEvent = function(a) {
        if ("1" == urlParams.dev) EditorUi.debug("logEvent", a);
        else if (EditorUi.enableLogging) try {
            var d = null != window.DRAWIO_LOG_URL ? window.DRAWIO_LOG_URL : "";
            (new Image).src = d + "/images/1x1.png?v=" + encodeURIComponent(EditorUi.VERSION) + (null != a ? "&data=" + encodeURIComponent(JSON.stringify(a)) : "")
        } catch (m) {}
    };
    EditorUi.sendReport = function(a, b) {
        if ("1" == urlParams.dev) EditorUi.debug("sendReport", a);
        else if (EditorUi.enableLogging) try {
            b = null != b ? b : 5E4, a.length > b && (a = a.substring(0, b) + "\n...[SHORTENED]"), mxUtils.post("/email", "version=" + encodeURIComponent(EditorUi.VERSION) + "&url=" + encodeURIComponent(window.location.href) + "&data=" + encodeURIComponent(a))
        } catch (m) {}
    };
    EditorUi.debug = function() {
        try {
            if (null != window.console && "1" == urlParams.test) {
                for (var a = [(new Date).toISOString()], b = 0; b < arguments.length; b++) null != arguments[b] && a.push(arguments[b]);
                console.log.apply(console, a)
            }
        } catch (m) {}
    };
    EditorUi.parsePng = function(a, b, c) {
        function d(a, d) {
            var b = g;
            g += d;
            return a.substring(b, g)
        }

        function e(a) {
            a = d(a, 4);
            return a.charCodeAt(3) + (a.charCodeAt(2) << 8) + (a.charCodeAt(1) << 16) + (a.charCodeAt(0) << 24)
        }
        var g = 0;
        if (d(a, 8) != String.fromCharCode(137) + "PNG" + String.fromCharCode(13, 10, 26, 10)) null != c && c();
        else if (d(a, 4), "IHDR" != d(a, 4)) null != c && c();
        else {
            d(a, 17);
            do {
                c = e(a);
                var m = d(a, 4);
                if (null != b && b(g - 8, m, c)) break;
                value = d(a, c);
                d(a, 4);
                if ("IEND" == m) break
            } while (c)
        }
    };
    EditorUi.removeChildNodes = function(a) {
        for (; null != a.firstChild;) a.removeChild(a.firstChild)
    };
    EditorUi.prototype.emptyDiagramXml = '<mxGraphModel><root><mxCell id="0"/><mxCell id="1" parent="0"/></root></mxGraphModel>';
    EditorUi.prototype.emptyLibraryXml = "<mxlibrary>[]</mxlibrary>";
    EditorUi.prototype.mode = null;
    EditorUi.prototype.timeout = Editor.prototype.timeout;
    EditorUi.prototype.sidebarFooterHeight = 38;
    EditorUi.prototype.defaultCustomShapeStyle = "shape=stencil(tZRtTsQgEEBPw1+DJR7AoN6DbWftpAgE0Ortd/jYRGq72R+YNE2YgTePloEJGWblgA18ZuKFDcMj5/Sm8boZq+BgjCX4pTyqk6ZlKROitwusOMXKQDODx5iy4pXxZ5qTHiFHawxB0JrQZH7lCabQ0Fr+XWC1/E8zcsT/gAi+Subo2/3Mh6d/oJb5nU1b5tW7r2knautaa3T+U32o7f7vZwpJkaNDLORJjcu7t59m2jXxqX9un+tt022acsfmoKaQZ+vhhswZtS6Ne/ThQGt0IV0N3Yyv6P3CeT9/tHO0XFI5cAE=);whiteSpace=wrap;html=1;";
    EditorUi.prototype.maxBackgroundSize = 1600;
    EditorUi.prototype.maxImageSize = 520;
    EditorUi.prototype.maxTextWidth = 520;
    EditorUi.prototype.resampleThreshold = 1E5;
    EditorUi.prototype.maxImageBytes = 1E6;
    EditorUi.prototype.maxBackgroundBytes = 25E5;
    EditorUi.prototype.maxTextBytes = 5E5;
    EditorUi.prototype.currentFile = null;
    EditorUi.prototype.printPdfExport = !1;
    EditorUi.prototype.pdfPageExport = !0;
    EditorUi.prototype.formatEnabled = "0" != urlParams.format;
    EditorUi.prototype.insertTemplateEnabled = !0;
    EditorUi.prototype.closableScratchpad = !0;
    (function() {
        EditorUi.prototype.useCanvasForExport = !1;
        EditorUi.prototype.jpgSupported = !1;
        try {
            var a = document.createElement("canvas");
            EditorUi.prototype.canvasSupported = !(!a.getContext || !a.getContext("2d"))
        } catch (p) {}
        try {
            var b = document.createElement("canvas"),
                c = new Image;
            c.onload = function() {
                try {
                    b.getContext("2d").drawImage(c, 0, 0);
                    var a = b.toDataURL("image/png");
                    EditorUi.prototype.useCanvasForExport = null != a && 6 < a.length
                } catch (t) {}
            };
            c.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1px" height="1px" version="1.1"><foreignObject pointer-events="all" width="1" height="1"><div xmlns="http://www.w3.org/1999/xhtml"></div></foreignObject></svg>')))
        } catch (p) {}
        try {
            b = document.createElement("canvas");
            b.width = b.height = 1;
            var e = b.toDataURL("image/jpeg");
            EditorUi.prototype.jpgSupported = null !== e.match("image/jpeg")
        } catch (p) {}
    })();
    EditorUi.prototype.openLink = function(a, b, c) {
        return this.editor.graph.openLink(a, b, c)
    };
    EditorUi.prototype.showSplash = function(a) {};
    EditorUi.prototype.getLocalData = function(a, b) {
        b(localStorage.getItem(a))
    };
    EditorUi.prototype.setLocalData = function(a, b, c) {
        localStorage.setItem(a, b);
        null != c && c()
    };
    EditorUi.prototype.removeLocalData = function(a, b) {
        localStorage.removeItem(a);
        b()
    };
    EditorUi.prototype.setMathEnabled = function(a) {
        this.editor.graph.mathEnabled = a;
        this.editor.updateGraphComponents();
        this.editor.graph.refresh();
        this.fireEvent(new mxEventObject("mathEnabledChanged"))
    };
    EditorUi.prototype.isMathEnabled = function(a) {
        return this.editor.graph.mathEnabled
    };
    EditorUi.prototype.isOfflineApp = function() {
        return "1" == urlParams.offline
    };
    EditorUi.prototype.isOffline = function(a) {
        return this.isOfflineApp() || !navigator.onLine || !a && "1" == urlParams.stealth
    };
    EditorUi.prototype.createSpinner = function(a, b, c) {
        c = null != c ? c : 24;
        var d = new Spinner({
                lines: 12,
                length: c,
                width: Math.round(c / 3),
                radius: Math.round(c / 2),
                rotate: 0,
                color: "dark" == uiTheme ? "#c0c0c0" : "#000",
                speed: 1.5,
                trail: 60,
                shadow: !1,
                hwaccel: !1,
                zIndex: 2E9
            }),
            e = d.spin;
        d.spin = function(c, g) {
            var m = !1;
            this.active || (e.call(this, c), this.active = !0, null != g && (m = document.createElement("div"), m.style.position = "absolute", m.style.whiteSpace = "nowrap", m.style.background = "#4B4243", m.style.color = "white", m.style.fontFamily = "Helvetica, Arial", m.style.fontSize = "14pt", m.style.padding = "6px", m.style.paddingLeft = "10px", m.style.paddingRight = "10px", m.style.zIndex = 2E9, m.style.left = Math.max(0, a) + "px", m.style.top = Math.max(0, b + 70) + "px", mxUtils.setPrefixedStyle(m.style, "borderRadius", "6px"), mxUtils.setPrefixedStyle(m.style, "transform", "translate(-50%,-50%)"), "dark" != uiTheme && mxUtils.setPrefixedStyle(m.style, "boxShadow", "2px 2px 3px 0px #ddd"), "..." != g.substring(g.length - 3, g.length) && "!" != g.charAt(g.length - 1) && (g += "..."), m.innerHTML = g, c.appendChild(m), d.status = m, mxClient.IS_VML && (null == document.documentMode || 8 >= document.documentMode) && (m.style.left = Math.round(Math.max(0, a - m.offsetWidth / 2)) + "px", m.style.top = Math.round(Math.max(0, b + 70 - m.offsetHeight / 2)) + "px")), this.pause = mxUtils.bind(this, function() {
                var a = function() {};
                this.active && (a = mxUtils.bind(this, function() {
                    this.spin(c, g)
                }));
                this.stop();
                return a
            }), m = !0);
            return m
        };
        var g = d.stop;
        d.stop = function() {
            g.call(this);
            this.active = !1;
            null != d.status && null != d.status.parentNode && d.status.parentNode.removeChild(d.status);
            d.status = null
        };
        d.pause = function() {
            return function() {}
        };
        return d
    };
    EditorUi.prototype.isCompatibleString = function(a) {
        try {
            var d = mxUtils.parseXml(a),
                b = this.editor.extractGraphModel(d.documentElement, !0);
            return null != b && 0 == b.getElementsByTagName("parsererror").length
        } catch (n) {}
        return !1
    };
    EditorUi.prototype.isVisioData = function(a) {
        return 8 < a.length && (208 == a.charCodeAt(0) && 207 == a.charCodeAt(1) && 17 == a.charCodeAt(2) && 224 == a.charCodeAt(3) && 161 == a.charCodeAt(4) && 177 == a.charCodeAt(5) && 26 == a.charCodeAt(6) && 225 == a.charCodeAt(7) || 80 == a.charCodeAt(0) && 75 == a.charCodeAt(1) && 3 == a.charCodeAt(2) && 4 == a.charCodeAt(3) || 80 == a.charCodeAt(0) && 75 == a.charCodeAt(1) && 3 == a.charCodeAt(2) && 6 == a.charCodeAt(3))
    };
    EditorUi.prototype.isRemoteVisioData = function(a) {
        return 8 < a.length && (208 == a.charCodeAt(0) && 207 == a.charCodeAt(1) && 17 == a.charCodeAt(2) && 224 == a.charCodeAt(3) && 161 == a.charCodeAt(4) && 177 == a.charCodeAt(5) && 26 == a.charCodeAt(6) && 225 == a.charCodeAt(7) || 60 == a.charCodeAt(0) && 63 == a.charCodeAt(1) && 120 == a.charCodeAt(2) && 109 == a.charCodeAt(3) && 108 == a.charCodeAt(3))
    };
    EditorUi.prototype.isPngData = function(a) {
        return 8 < a.length && 137 == a.charCodeAt(0) && 80 == a.charCodeAt(1) && 78 == a.charCodeAt(2) && 71 == a.charCodeAt(3) && 13 == a.charCodeAt(4) && 10 == a.charCodeAt(5) && 26 == a.charCodeAt(6) && 10 == a.charCodeAt(7)
    };
    var a = EditorUi.prototype.extractGraphModelFromHtml;
    EditorUi.prototype.extractGraphModelFromHtml = function(d) {
        var b = a.apply(this, arguments);
        if (null == b) try {
            var c = d.indexOf("&lt;mxfile ");
            if (0 <= c) {
                var e = d.lastIndexOf("&lt;/mxfile&gt;");
                e > c && (b = d.substring(c, e + 15).replace(/&gt;/g, ">").replace(/&lt;/g, "<").replace(/\\&quot;/g, '"').replace(/\n/g, ""))
            } else var f = mxUtils.parseXml(d),
                k = this.editor.extractGraphModel(f.documentElement, null != this.pages || "hidden" == this.diagramContainer.style.visibility),
                b = null != k ? mxUtils.getXml(k) : ""
        } catch (u) {}
        return b
    };
    EditorUi.prototype.validateFileData = function(a) {
        if (null != a && 0 < a.length) {
            var d = a.indexOf('<meta charset="utf-8">');
            0 <= d && (a = a.slice(0, d) + '<meta charset="utf-8"/>' + a.slice(d + 23 - 1, a.length));
            a = Graph.zapGremlins(a)
        }
        return a
    };
    EditorUi.prototype.replaceFileData = function(a) {
        a = this.validateFileData(a);
        a = null != a && 0 < a.length ? mxUtils.parseXml(a).documentElement : null;
        var d = null != a ? this.editor.extractGraphModel(a, !0) : null;
        null != d && (a = d);
        if (null != a) {
            d = this.editor.graph;
            d.model.beginUpdate();
            try {
                var b = null != this.pages ? this.pages.slice() : null,
                    c = a.getElementsByTagName("diagram");
                if ("0" != urlParams.pages || 1 < c.length || 1 == c.length && c[0].hasAttribute("name")) {
                    this.fileNode = a;
                    this.pages = null != this.pages ? this.pages : [];
                    for (var e = c.length - 1; 0 <= e; e--) {
                        var f = this.updatePageRoot(new DiagramPage(c[e]));
                        null == f.getName() && f.setName(mxResources.get("pageWithNumber", [e + 1]));
                        d.model.execute(new ChangePage(this, f, 0 == e ? f : null, 0))
                    }
                } else "0" != urlParams.pages && null == this.fileNode && (this.fileNode = a.ownerDocument.createElement("mxfile"), this.currentPage = new DiagramPage(a.ownerDocument.createElement("diagram")), this.currentPage.setName(mxResources.get("pageWithNumber", [1])), d.model.execute(new ChangePage(this, this.currentPage, this.currentPage, 0))), this.editor.setGraphXml(a), null != this.currentPage && (this.currentPage.root = this.editor.graph.model.root);
                if (null != b)
                    for (e = 0; e < b.length; e++) d.model.execute(new ChangePage(this, b[e], null))
            } finally {
                d.model.endUpdate()
            }
        }
    };
    EditorUi.prototype.createFileData = function(a, b, c, e, f, k, l, q, v, x, y) {
        b = null != b ? b : this.editor.graph;
        f = null != f ? f : !1;
        v = null != v ? v : !0;
        var d, g = null;
        null == c || c.getMode() == App.MODE_DEVICE || c.getMode() == App.MODE_BROWSER ? d = "_blank" : g = d = e;
        if (null == a) return "";
        var m = a;
        if ("mxfile" != m.nodeName.toLowerCase()) {
            if (y) {
                var n = a.ownerDocument.createElement("diagram");
                n.setAttribute("id", Editor.guid());
                n.appendChild(a)
            } else {
                n = Graph.zapGremlins(mxUtils.getXml(a));
                m = Graph.compress(n);
                if (Graph.decompress(m) != n) return n;
                n = a.ownerDocument.createElement("diagram");
                n.setAttribute("id", Editor.guid());
                mxUtils.setTextContent(n, m)
            }
            m = a.ownerDocument.createElement("mxfile");
            m.appendChild(n)
        }
        x ? (m = m.cloneNode(!0),
            m.removeAttribute("modified"),
            m.removeAttribute("host"),
            m.removeAttribute("agent"),
            m.removeAttribute("etag"),
            m.removeAttribute("userAgent"),
            m.removeAttribute("version"),
            m.removeAttribute("editor"),
            m.removeAttribute("type")) : (m.removeAttribute("userAgent"),
            m.removeAttribute("version"),
            m.removeAttribute("editor"),
            m.removeAttribute("pages"),
            m.removeAttribute("type"),
            mxClient.IS_CHROMEAPP ? m.setAttribute("host", "Chrome") : EditorUi.isElectronApp ? m.setAttribute("host", "Electron") : m.setAttribute("host", window.location.hostname),
            m.setAttribute("modified", (new Date).toISOString()),
            m.setAttribute("agent", ""),
            m.setAttribute("version", EditorUi.VERSION),
            m.setAttribute("etag", Editor.guid()), a = null != c ? c.getMode() : this.mode, null != a && m.setAttribute("type", a), 1 < m.getElementsByTagName("diagram").length && null != this.pages && m.setAttribute("pages", this.pages.length));
        y = y ? mxUtils.getPrettyXml(m) : mxUtils.getXml(m);
        if (!k && !f && (l || null != c && /(\.html)$/i.test(c.getTitle()))) y = this.getHtml2(mxUtils.getXml(m), b, null != c ? c.getTitle() : null, d, g);
        else if (k || !f && null != c && /(\.svg)$/i.test(c.getTitle())) null == c || c.getMode() != App.MODE_DEVICE && c.getMode() != App.MODE_BROWSER || (e = null), y = this.getEmbeddedSvg(y, b, e, null, q, v, g);
        return y
    };


    EditorUi.prototype.getXmlFileData = function(a, b, c) {
        a = null != a ? a : !0;
        b = null != b ? b : !1;
        c = null != c ? c : !Editor.compressXml;
        var d = this.editor.getGraphXml(a);
        if (a && null != this.fileNode && null != this.currentPage)
            if (a = function(a) {
                    var b = a.getElementsByTagName("mxGraphModel"),
                        b = 0 < b.length ? b[0] : null;
                    null == b && c ? (b = mxUtils.trim(mxUtils.getTextContent(a)), a = a.cloneNode(!1), 0 < b.length && (b = Graph.decompress(b), null != b && 0 < b.length && a.appendChild(mxUtils.parseXml(b).documentElement))) : null == b || c ? a = a.cloneNode(!0) : (a = a.cloneNode(!1), mxUtils.setTextContent(a, Graph.compressNode(b)));
                    d.appendChild(a)
                }, EditorUi.removeChildNodes(this.currentPage.node), mxUtils.setTextContent(this.currentPage.node, Graph.compressNode(d)), d = this.fileNode.cloneNode(!1), b) a(this.currentPage.node);
            else
                for (b = 0; b < this.pages.length; b++) {
                    if (this.currentPage != this.pages[b] && this.pages[b].needsUpdate) {
                        var e = (new mxCodec(mxUtils.createXmlDocument())).encode(new mxGraphModel(this.pages[b].root));
                        this.editor.graph.saveViewState(this.pages[b].viewState, e);
                        EditorUi.removeChildNodes(this.pages[b].node);
                        mxUtils.setTextContent(this.pages[b].node, Graph.compressNode(e));
                        delete this.pages[b].needsUpdate
                    }
                    a(this.pages[b].node)
                }
        return d
    };
    EditorUi.prototype.anonymizeString = function(a, b) {
        for (var d = [], c = 0; c < a.length; c++) {
            var e = a.charAt(c);
            0 <= EditorUi.ignoredAnonymizedChars.indexOf(e) ? d.push(e) : isNaN(parseInt(e)) ? e.toLowerCase() != e ? d.push(String.fromCharCode(65 + Math.round(25 * Math.random()))) : e.toUpperCase() != e ? d.push(String.fromCharCode(97 + Math.round(25 * Math.random()))) : /\s/.test(e) ? d.push(" ") : d.push("?") : d.push(b ? "0" : Math.round(9 * Math.random()))
        }
        return d.join("")
    };
    EditorUi.prototype.anonymizePatch = function(a) {
        if (null != a[EditorUi.DIFF_INSERT])
            for (var d = 0; d < a[EditorUi.DIFF_INSERT].length; d++) try {
                var b = mxUtils.parseXml(a[EditorUi.DIFF_INSERT][d].data).documentElement.cloneNode(!1);
                null != b.getAttribute("name") && b.setAttribute("name", this.anonymizeString(b.getAttribute("name")));
                a[EditorUi.DIFF_INSERT][d].data = mxUtils.getXml(b)
            } catch (t) {
                a[EditorUi.DIFF_INSERT][d].data = t.message
            }
        if (null != a[EditorUi.DIFF_UPDATE]) {
            for (var c in a[EditorUi.DIFF_UPDATE]) {
                var e = a[EditorUi.DIFF_UPDATE][c];
                null != e.name && (e.name = this.anonymizeString(e.name));
                null != e.cells && (d = mxUtils.bind(this, function(a) {
                    var d = e.cells[a];
                    if (null != d) {
                        for (var b in d) null != d[b].value && (d[b].value = "[" + d[b].value.length + "]"), null != d[b].xmlValue && (d[b].xmlValue = "[" + d[b].xmlValue.length + "]"), null != d[b].style && (d[b].style = "[" + d[b].style.length + "]"), 0 == Object.keys(d[b]).length && delete d[b];
                        0 == Object.keys(d).length && delete e.cells[a]
                    }
                }), d(EditorUi.DIFF_INSERT), d(EditorUi.DIFF_UPDATE), 0 == Object.keys(e.cells).length && delete e.cells);
                0 == Object.keys(e).length && delete a[EditorUi.DIFF_UPDATE][c]
            }
            0 == Object.keys(a[EditorUi.DIFF_UPDATE]).length && delete a[EditorUi.DIFF_UPDATE]
        }
        return a
    };
    EditorUi.prototype.anonymizeAttributes = function(a, b) {
        if (null != a.attributes)
            for (var d = 0; d < a.attributes.length; d++) "as" != a.attributes[d].name && a.setAttribute(a.attributes[d].name, this.anonymizeString(a.attributes[d].value, b));
        if (null != a.childNodes)
            for (d = 0; d < a.childNodes.length; d++) this.anonymizeAttributes(a.childNodes[d], b)
    };
    EditorUi.prototype.anonymizeNode = function(a, b) {
        for (var d = a.getElementsByTagName("mxCell"), c = 0; c < d.length; c++) null != d[c].getAttribute("value") && d[c].setAttribute("value", "[" + d[c].getAttribute("value").length + "]"), null != d[c].getAttribute("xmlValue") && d[c].setAttribute("xmlValue", "[" + d[c].getAttribute("xmlValue").length + "]"), null != d[c].getAttribute("style") && d[c].setAttribute("style", "[" + d[c].getAttribute("style").length + "]"), null != d[c].parentNode && "root" != d[c].parentNode.nodeName && null != d[c].parentNode.parentNode && (d[c].setAttribute("id", d[c].parentNode.getAttribute("id")), d[c].parentNode.parentNode.replaceChild(d[c], d[c].parentNode));
        return a
    };
    EditorUi.prototype.synchronizeCurrentFile = function(a) {
        var d = this.getCurrentFile();
        null != d && (d.savingFile ? this.handleError({
            message: mxResources.get("busy")
        }) : !a && d.invalidChecksum ? d.handleFileError(null, !0) : this.spinner.spin(document.body, mxResources.get("updatingDocument")) && (d.clearAutosave(), this.editor.setStatus(""), a ? d.reloadFile(mxUtils.bind(this, function() {
            d.handleFileSuccess("manual" == DrawioFile.SYNC)
        }), mxUtils.bind(this, function(a) {
            d.handleFileError(a, !0)
        })) : d.synchronizeFile(mxUtils.bind(this, function() {
            d.handleFileSuccess("manual" == DrawioFile.SYNC)
        }), mxUtils.bind(this, function(a) {
            d.handleFileError(a, !0)
        }))))
    };
    EditorUi.prototype.getFileData = function(a, b, c, e, f, k, l, q, v, x) {
        f = null != f ? f : !0;
        k = null != k ? k : !1;
        var d = this.editor.graph;
        if (b || !a && null != v && /(\.svg)$/i.test(v.getTitle()))
            if (x = !1, null != this.pages && this.currentPage != this.pages[0]) {
                var g = d.getGlobalVariable,
                    d = this.createTemporaryGraph(d.getStylesheet()),
                    m = this.pages[0];
                d.getGlobalVariable = function(a) {
                    return "page" == a ? m.getName() : "pagenumber" == a ? 1 : g.apply(this, arguments)
                };
                document.body.appendChild(d.container);
                d.model.setRoot(m.root)
            }
        l = null != l ? l : this.getXmlFileData(f, k, x);
        v = null != v ? v : this.getCurrentFile();
        a = this.createFileData(l, d, v, window.location.href, a, b, c, e, f, q, x);
        d != this.editor.graph && d.container.parentNode.removeChild(d.container);
        return a
    };
    EditorUi.prototype.getHtml = function(a, b, c, e, f, k) {
        k = null != k ? k : !0;
        var d = null,
            g = EditorUi.drawHost + "/js/embed-static.min.js";
        if (null != b) {
            var d = k ? b.getGraphBounds() : b.getBoundingBox(b.getSelectionCells()),
                m = b.view.scale;
            k = Math.floor(d.x / m - b.view.translate.x);
            m = Math.floor(d.y / m - b.view.translate.y);
            d = b.background;
            null == f && (b = this.getBasenames().join(";"), 0 < b.length && (g = EditorUi.drawHost + "/embed.js?s=" + b));
            a.setAttribute("x0", k);
            a.setAttribute("y0", m)
        }
        null != a && (a.setAttribute("pan", "1"), a.setAttribute("zoom", "1"), a.setAttribute("resize", "0"), a.setAttribute("fit", "0"), a.setAttribute("border", "20"), a.setAttribute("links", "1"), null != e && a.setAttribute("edit", e));
        null != f && (f = f.replace(/&/g, "&amp;"));
        a = null != a ? Graph.zapGremlins(mxUtils.getXml(a)) : "";
        e = Graph.compress(a);
        Graph.decompress(e) != a && (e = encodeURIComponent(a));
        return (null == f ? '<!--[if IE]><meta http-equiv="X-UA-Compatible" content="IE=5,IE=9" ><![endif]-->\n' : "") + "<!DOCTYPE html>\n<html" + (null != f ? ' xmlns="http://www.w3.org/1999/xhtml">' : ">") + "\n<head>\n" + (null == f ? null != c ? "<title>" + mxUtils.htmlEntities(c) + "</title>\n" : "" : "<title>diagrams.net</title>\n") + (null != f ? '<meta http-equiv="refresh" content="0;URL=\'' + f + "'\"/>\n" : "") + "</head>\n<body" + (null == f && null != d && d != mxConstants.NONE ? ' style="background-color:' + d + ';">' : ">") + '\n<div class="mxgraph" style="position:relative;overflow:auto;width:100%;">\n<div style="width:1px;height:1px;overflow:hidden;">' + e + "</div>\n</div>\n" + (null == f ? '<script type="text/javascript" src="' + g + '"></script>' : '<a style="position:absolute;top:50%;left:50%;margin-top:-128px;margin-left:-64px;" href="' + f + '" target="_blank"><img border="0" src="' + EditorUi.drawHost + '/images/drawlogo128.png"/></a>') + "\n</body>\n</html>\n"
    };
    EditorUi.prototype.getHtml2 = function(a, b, c, e, f) {
        b = window.DRAWIO_VIEWER_URL || EditorUi.drawHost + "/js/viewer-static.min.js";
        null != f && (f = f.replace(/&/g, "&amp;"));
        a = {
            highlight: "#0000ff",
            nav: this.editor.graph.foldingEnabled,
            resize: !0,
            xml: Graph.zapGremlins(a),
            toolbar: "pages zoom layers lightbox"
        };
        null != this.pages && null != this.currentPage && (a.page = mxUtils.indexOf(this.pages, this.currentPage));
        return (null == f ? '<!--[if IE]><meta http-equiv="X-UA-Compatible" content="IE=5,IE=9" ><![endif]-->\n' : "") + "<!DOCTYPE html>\n<html" + (null != f ? ' xmlns="http://www.w3.org/1999/xhtml">' : ">") + "\n<head>\n" + (null == f ? null != c ? "<title>" + mxUtils.htmlEntities(c) + "</title>\n" : "" : "<title>diagrams.net</title>\n") + (null != f ? '<meta http-equiv="refresh" content="0;URL=\'' + f + "'\"/>\n" : "") + '<meta charset="utf-8"/>\n</head>\n<body>\n<div class="mxgraph" style="max-width:100%;border:1px solid transparent;" data-mxgraph="' + mxUtils.htmlEntities(JSON.stringify(a)) + '"></div>\n' + (null == f ? '<script type="text/javascript" src="' + b + '"></script>' : '<a style="position:absolute;top:50%;left:50%;margin-top:-128px;margin-left:-64px;" href="' + f + '" target="_blank"><img border="0" src="' + EditorUi.drawHost + '/images/drawlogo128.png"/></a>') + "\n</body>\n</html>\n"
    };
    EditorUi.prototype.setFileData = function(a) {
        a = this.validateFileData(a);
        this.pages = this.fileNode = this.currentPage = null;
        a = null != a && 0 < a.length ? mxUtils.parseXml(a).documentElement : null;
        var d = Editor.extractParserError(a, mxResources.get("invalidOrMissingFile"));
        if (d) throw Error(mxResources.get("notADiagramFile") + " (" + d + ")");
        d = null != a ? this.editor.extractGraphModel(a, !0) : null;
        null != d && (a = d);
        if (null != a && "mxfile" == a.nodeName && (d = a.getElementsByTagName("diagram"), "0" != urlParams.pages || 1 < d.length || 1 == d.length && d[0].hasAttribute("name"))) {
            var b = null;
            this.fileNode = a;
            this.pages = [];
            for (var c = 0; c < d.length; c++) null == d[c].getAttribute("id") && d[c].setAttribute("id", c), a = new DiagramPage(d[c]),
                null == a.getName() && a.setName(mxResources.get("pageWithNumber", [c + 1])), this.pages.push(a), null != urlParams["page-id"] && a.getId() == urlParams["page-id"] && (b = a);
            this.currentPage = null != b ? b : this.pages[Math.max(0, Math.min(this.pages.length - 1, urlParams.page || 0))];
            a = this.currentPage.node
        }
        "0" != urlParams.pages && null == this.fileNode && null != a && (this.fileNode = a.ownerDocument.createElement("mxfile"), this.currentPage = new DiagramPage(a.ownerDocument.createElement("diagram")), this.currentPage.setName(mxResources.get("pageWithNumber", [1])), this.pages = [this.currentPage]);
        this.editor.setGraphXml(a);
        null != this.currentPage && (this.currentPage.root = this.editor.graph.model.root);
        if (null != urlParams["layer-ids"]) try {
            var e = urlParams["layer-ids"].split(" ");
            a = {};
            for (c = 0; c < e.length; c++) a[e[c]] = !0;
            for (var f = this.editor.graph.getModel(), k = f.getChildren(f.root), c = 0; c < k.length; c++) {
                var l = k[c];
                f.setVisible(l, a[l.id] || !1)
            }
        } catch (v) {}
    };
    EditorUi.prototype.getBaseFilename = function(a) {
        var d = this.getCurrentFile(),
            d = null != d && null != d.getTitle() ? d.getTitle() : this.defaultFilename;
        if (/(\.xml)$/i.test(d) || /(\.html)$/i.test(d) || /(\.svg)$/i.test(d) || /(\.png)$/i.test(d) || /(\.drawio)$/i.test(d)) d = d.substring(0, d.lastIndexOf("."));
        !a && null != this.pages && 1 < this.pages.length && null != this.currentPage && null != this.currentPage.node.getAttribute("name") && 0 < this.currentPage.getName().length && (d = d + "-" + this.currentPage.getName());
        return d
    };
    EditorUi.prototype.downloadFile = function(a, b, c, e, f, k, l, q, v, x, y) {
        try {
            e = null != e ? e : this.editor.graph.isSelectionEmpty();
            var d = this.getBaseFilename(!f),
                g = d + "." + a;
            if ("xml" == a) {
                var m = '<?xml version="1.0" encoding="UTF-8"?>\n' + this.getFileData(!0, null, null, null, e, f, null, null, null, b);
                this.saveData(g, a, m, "text/xml")
            } else if ("html" == a) m = this.getHtml2(this.getFileData(!0), this.editor.graph, d), this.saveData(g, a, m, "text/html");
            else if ("svg" != a && "xmlsvg" != a || !this.spinner.spin(document.body, mxResources.get("export"))) "xmlpng" == a ? g = d + ".png" : "jpeg" == a && (g = d + ".jpg"), this.saveRequest(g, a, mxUtils.bind(this, function(d, b) {
                try {
                    var c = this.editor.graph.pageVisible;
                    null != k && (this.editor.graph.pageVisible = k);
                    var g = this.createDownloadRequest(d, a, e, b, l, f, q, v, x, y);
                    this.editor.graph.pageVisible = c;
                    return g
                } catch (L) {
                    this.handleError(L)
                }
            }));
            else {
                var n = null,
                    p = mxUtils.bind(this, function(a) {
                        a.length <= MAX_REQUEST_SIZE ? this.saveData(g, "svg", a, "image/svg+xml") : this.handleError({
                            message: mxResources.get("drawingTooLarge")
                        }, mxResources.get("error"), mxUtils.bind(this, function() {
                            mxUtils.popup(n)
                        }))
                    });
                if ("svg" == a) {
                    var t = this.editor.graph.background;
                    if (l || t == mxConstants.NONE) t = null;
                    var u = this.editor.graph.getSvg(t, null, null, null, null, e);
                    c && this.editor.graph.addSvgShadow(u);
                    this.editor.convertImages(u, mxUtils.bind(this, mxUtils.bind(this, function(a) {
                        this.spinner.stop();
                        p('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n' + mxUtils.getXml(a))
                    })))
                } else g = d + ".svg", n = this.getFileData(!1, !0, null, mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    p(a)
                }), e)
            }
        } catch (H) {
            this.handleError(H)
        }
    };
    EditorUi.prototype.createDownloadRequest = function(a, b, c, e, f, k, l, q, v, x) {
        var d = this.editor.graph,
            g = d.getGraphBounds();
        c = this.getFileData(!0, null, null, null, c, 0 == k ? !1 : "xmlpng" != b);
        var m = "",
            n = "";
        if (g.width * g.height > MAX_AREA || c.length > MAX_REQUEST_SIZE) throw {
            message: mxResources.get("drawingTooLarge")
        };
        x = x ? "1" : "0";
        "pdf" == b && 0 == k && (n = "&allPages=1");
        if ("xmlpng" == b && (x = "1", b = "png", null != this.pages && null != this.currentPage))
            for (k = 0; k < this.pages.length; k++)
                if (this.pages[k] == this.currentPage) {
                    m = "&from=" + k;
                    break
                }
        k = d.background;
        "png" != b && "pdf" != b || !f ? f || null != k && k != mxConstants.NONE || (k = "#ffffff") : k = mxConstants.NONE;
        f = {
            globalVars: d.getExportVariables()
        };
        v && (f.grid = {
            size: d.gridSize,
            steps: d.view.gridSteps,
            color: d.view.gridColor
        });
        return new mxXmlRequest(EXPORT_URL, "format=" + b + m + n + "&bg=" + (null != k ? k : mxConstants.NONE) + "&base64=" + e + "&embedXml=" + x + "&xml=" + encodeURIComponent(c) + (null != a ? "&filename=" + encodeURIComponent(a) : "") + "&extras=" + encodeURIComponent(JSON.stringify(f)) + (null != l ? "&scale=" + l : "") + (null != q ? "&border=" + q : ""))
    };
    EditorUi.prototype.setMode = function(a, b) {
        this.mode = a
    };
    EditorUi.prototype.loadDescriptor = function(a, b, c) {
        var d = window.location.hash,
            e = mxUtils.bind(this, function(c) {
                var e = null != a.data ? a.data : "";
                null != c && 0 < c.length && (0 < e.length && (e += "\n"), e += c);
                c = new LocalFile(this, "csv" != a.format && 0 < e.length ? e : this.emptyDiagramXml, null != urlParams.title ? decodeURIComponent(urlParams.title) : this.defaultFilename, !0);
                c.getHash = function() {
                    return d
                };
                this.fileLoaded(c);
                "csv" == a.format && this.importCsv(e, mxUtils.bind(this, function(a) {
                    this.editor.undoManager.clear();
                    this.editor.setModified(!1);
                    this.editor.setStatus("")
                }));
                if (null != a.update) {
                    var g = null != a.interval ? parseInt(a.interval) : 6E4,
                        m = null,
                        f = mxUtils.bind(this, function() {
                            var d = this.currentPage;
                            mxUtils.post(a.update, "xml=" + encodeURIComponent(mxUtils.getXml(this.editor.getGraphXml())), mxUtils.bind(this, function(a) {
                                d === this.currentPage && (200 <= a.getStatus() && 300 >= a.getStatus() ? (this.updateDiagram(a.getText()), n()) : this.handleError({
                                    message: mxResources.get("error") + " " + a.getStatus()
                                }))
                            }), mxUtils.bind(this, function(a) {
                                this.handleError(a)
                            }))
                        }),
                        n = mxUtils.bind(this, function() {
                            window.clearTimeout(m);
                            m = window.setTimeout(f, g)
                        });
                    this.editor.addListener("pageSelected", mxUtils.bind(this, function() {
                        n();
                        f()
                    }));
                    n();
                    f()
                }
                null != b && b()
            });
        if (null != a.url && 0 < a.url.length) {
            var g = a.url;
            /^https?:\/\//.test(g) && !this.editor.isCorsEnabledForUrl(g) && (g = PROXY_URL + "?url=" + encodeURIComponent(g));
            this.editor.loadUrl(g, mxUtils.bind(this, function(a) {
                e(a)
            }), mxUtils.bind(this, function(a) {
                null != c && c(a)
            }))
        } else e("")
    };
    EditorUi.prototype.updateDiagram = function(a) {
        function d(a) {
            var d = new mxCellOverlay(a.image || e.warningImage, a.tooltip, a.align, a.valign, a.offset);
            d.addListener(mxEvent.CLICK, function(d, b) {
                c.alert(a.tooltip)
            });
            return d
        }
        var b = null,
            c = this;
        if (null != a && 0 < a.length && (b = mxUtils.parseXml(a), a = null != b ? b.documentElement : null, null != a && "updates" == a.nodeName)) {
            var e = this.editor.graph,
                f = e.getModel();
            f.beginUpdate();
            var k = null;
            try {
                for (a = a.firstChild; null != a;) {
                    if ("update" == a.nodeName) {
                        var l = f.getCell(a.getAttribute("id"));
                        if (null != l) {
                            try {
                                var v = a.getAttribute("value");
                                if (null != v) {
                                    var x = mxUtils.parseXml(v).documentElement;
                                    if (null != x)
                                        if ("1" == x.getAttribute("replace-value")) f.setValue(l, x);
                                        else
                                            for (var y = x.attributes, A = 0; A < y.length; A++) e.setAttributeForCell(l, y[A].nodeName, 0 < y[A].nodeValue.length ? y[A].nodeValue : null)
                                }
                            } catch (I) {
                                null != window.console && console.log("Error in value for " + l.id + ": " + I)
                            }
                            try {
                                var z = a.getAttribute("style");
                                null != z && e.model.setStyle(l, z)
                            } catch (I) {
                                null != window.console && console.log("Error in style for " + l.id + ": " + I)
                            }
                            try {
                                var B = a.getAttribute("icon");
                                if (null != B) {
                                    var C = 0 < B.length ? JSON.parse(B) : null;
                                    null != C && C.append || e.removeCellOverlays(l);
                                    null != C && e.addCellOverlay(l, d(C))
                                }
                            } catch (I) {
                                null != window.console && console.log("Error in icon for " + l.id + ": " + I)
                            }
                            try {
                                var D = a.getAttribute("geometry");
                                if (null != D) {
                                    var D = JSON.parse(D),
                                        E = e.getCellGeometry(l);
                                    if (null != E) {
                                        E = E.clone();
                                        for (key in D) {
                                            var F = parseFloat(D[key]);
                                            "dx" == key ? E.x += F : "dy" == key ? E.y += F : "dw" == key ? E.width += F : "dh" == key ? E.height += F : E[key] = parseFloat(D[key])
                                        }
                                        e.model.setGeometry(l, E)
                                    }
                                }
                            } catch (I) {
                                null != window.console && console.log("Error in icon for " + l.id + ": " + I)
                            }
                        }
                    } else if ("model" == a.nodeName) {
                        for (var H = a.firstChild; null != H && H.nodeType != mxConstants.NODETYPE_ELEMENT;) H = H.nextSibling;
                        null != H && (new mxCodec(a.firstChild)).decode(H, f)
                    } else if ("view" == a.nodeName) {
                        if (a.hasAttribute("scale") && (e.view.scale = parseFloat(a.getAttribute("scale"))), a.hasAttribute("dx") || a.hasAttribute("dy")) e.view.translate = new mxPoint(parseFloat(a.getAttribute("dx") || 0), parseFloat(a.getAttribute("dy") || 0))
                    } else "fit" == a.nodeName && (k = a.hasAttribute("max-scale") ? parseFloat(a.getAttribute("max-scale")) : 1);
                    a = a.nextSibling
                }
            } finally {
                f.endUpdate()
            }
            null != k && this.chromelessResize && this.chromelessResize(!0, k)
        }
        return b
    };
    EditorUi.prototype.getCopyFilename = function(a, b) {
        var d = null != a && null != a.getTitle() ? a.getTitle() : this.defaultFilename,
            c = "",
            e = d.lastIndexOf(".");
        0 <= e && (c = d.substring(e), d = d.substring(0, e));
        if (b) var g = new Date,
            e = g.getFullYear(),
            f = g.getMonth() + 1,
            k = g.getDate(),
            l = g.getHours(),
            x = g.getMinutes(),
            g = g.getSeconds(),
            d = d + (" " + (e + "-" + f + "-" + k + "-" + l + "-" + x + "-" + g));
        return d = mxResources.get("copyOf", [d]) + c
    };
    EditorUi.prototype.fileLoaded = function(a, b) {
        var d = this.getCurrentFile();
        this.fileLoadedError = null;
        this.setCurrentFile(null);
        var c = !1;
        this.hideDialog();
        null != d && (EditorUi.debug("File.closed", [d]), d.removeListener(this.descriptorChangedListener), d.close());
        this.editor.graph.model.clear();
        this.editor.undoManager.clear();
        var e = mxUtils.bind(this, function() {
            this.setGraphEnabled(!1);
            this.setCurrentFile(null);
            null != d && this.updateDocumentTitle();
            this.editor.graph.model.clear();
            this.editor.undoManager.clear();
            this.setBackgroundImage(null);
            !b && null != window.location.hash && 0 < window.location.hash.length && (window.location.hash = "");
            null != this.fname && (this.fnameWrapper.style.display = "none", this.fname.innerHTML = "", this.fname.setAttribute("title", mxResources.get("rename")));
            this.editor.setStatus("");
            this.updateUi();
            b || this.showSplash()
        });
        if (null != a) try {
            mxClient.IS_SF && "min" == uiTheme && (this.diagramContainer.style.visibility = "");
            this.openingFile = !0;
            this.setCurrentFile(a);
            a.addListener("descriptorChanged", this.descriptorChangedListener);
            a.addListener("contentChanged", this.descriptorChangedListener);
            a.open();
            delete this.openingFile;
            this.setGraphEnabled(!0);
            this.setMode(a.getMode());
            this.editor.graph.model.prefix = Editor.guid() + "-";
            this.editor.undoManager.clear();
            this.descriptorChanged();
            this.updateUi();
            a.isEditable() ? a.isModified() ? (a.addUnsavedStatus(), null != a.backupPatch && a.patch([a.backupPatch])) : this.editor.setStatus("") : this.editor.setStatus('<span class="geStatusAlert" style="margin-left:8px;">' + mxUtils.htmlEntities(mxResources.get("readOnly")) + "</span>");
            !this.editor.isChromelessView() || this.editor.editable ? (this.editor.graph.selectUnlockedLayer(), this.showLayersDialog(), this.restoreLibraries(), window.self !== window.top && window.focus()) : this.editor.graph.isLightboxView() && this.lightboxFit();
            this.chromelessResize && this.chromelessResize();
            this.editor.fireEvent(new mxEventObject("fileLoaded"));
            c = !0;
            this.isOffline() || null == a.getMode() || EditorUi.logEvent({
                category: a.getMode().toUpperCase() + "-OPEN-FILE-" + a.getHash(),
                action: "size_" + a.getSize(),
                label: "autosave_" + (this.editor.autosave ? "on" : "off")
            });
            EditorUi.debug("File.opened", [a]);
            if (this.editor.editable && this.mode == a.getMode() && a.getMode() != App.MODE_DEVICE && null != a.getMode()) try {
                this.addRecent({
                    id: a.getHash(),
                    title: a.getTitle(),
                    mode: a.getMode()
                })
            } catch (u) {}
            try {
                mxSettings.setOpenCounter(mxSettings.getOpenCounter() + 1), mxSettings.save()
            } catch (u) {}
        } catch (u) {
            this.fileLoadedError = u;
            if (EditorUi.enableLogging && !this.isOffline()) try {
                EditorUi.logEvent({
                    category: "ERROR-LOAD-FILE-" + (null != a ? a.getHash() : "none"),
                    action: "message_" + u.message,
                    label: "stack_" + u.stack
                })
            } catch (q) {}
            var g = mxUtils.bind(this, function() {
                null != urlParams.url && this.spinner.spin(document.body, mxResources.get("reconnecting")) ? window.location.search = this.getSearch(["url"]) : null != d ? this.fileLoaded(d) : e()
            });
            b ? g() : this.handleError(u, mxResources.get("errorLoadingFile"), g, !0, null, null, !0)
        } else e();
        return c
    };
    EditorUi.prototype.getHashValueForPages = function(a, b) {
        var d = 0,
            c = new mxGraphModel,
            e = new mxCodec;
        null != b && (b.byteCount = 0, b.attrCount = 0, b.eltCount = 0, b.nodeCount = 0);
        for (var g = 0; g < a.length; g++) {
            this.updatePageRoot(a[g]);
            var f = a[g].node.cloneNode(!1);
            f.removeAttribute("name");
            c.root = a[g].root;
            var k = e.encode(c);
            this.editor.graph.saveViewState(a[g].viewState, k, !0);
            k.removeAttribute("pageWidth");
            k.removeAttribute("pageHeight");
            f.appendChild(k);
            null != b && (b.eltCount += f.getElementsByTagName("*").length, b.nodeCount += f.getElementsByTagName("mxCell").length);
            d = (d << 5) - d + this.hashValue(f, function(a, d, b, c) {
                return !c || "mxGeometry" != a.nodeName && "mxPoint" != a.nodeName || "x" != d && "y" != d && "width" != d && "height" != d ? c && "mxCell" == a.nodeName && "previous" == d ? null : b : Math.round(b)
            }, b) << 0
        }
        return d
    };
    EditorUi.prototype.hashValue = function(a, b, c) {
        var d = 0;
        if (null != a && "object" === typeof a && "number" === typeof a.nodeType && "string" === typeof a.nodeName && "function" === typeof a.getAttribute) {
            null != a.nodeName && (d ^= this.hashValue(a.nodeName, b, c));
            if (null != a.attributes) {
                null != c && (c.attrCount += a.attributes.length);
                for (var e = 0; e < a.attributes.length; e++) {
                    var g = a.attributes[e].name,
                        f = null != b ? b(a, g, a.attributes[e].value, !0) : a.attributes[e].value;
                    null != f && (d ^= this.hashValue(g, b, c) + this.hashValue(f, b, c))
                }
            }
            if (null != a.childNodes)
                for (e = 0; e < a.childNodes.length; e++) d = (d << 5) - d + this.hashValue(a.childNodes[e], b, c) << 0
        } else if (null != a && "function" !== typeof a) {
            a = String(a);
            b = 0;
            null != c && (c.byteCount += a.length);
            for (e = 0; e < a.length; e++) b = (b << 5) - b + a.charCodeAt(e) << 0;
            d ^= b
        }
        return d
    };
    EditorUi.prototype.descriptorChanged = function() {};
    EditorUi.prototype.restoreLibraries = function() {};
    EditorUi.prototype.saveLibrary = function(a, b, c, e, f, k, l) {};
    EditorUi.prototype.isScratchpadEnabled = function() {
        return isLocalStorage || mxClient.IS_CHROMEAPP
    };
    EditorUi.prototype.toggleScratchpad = function() {
        this.isScratchpadEnabled() && (null == this.scratchpad ? StorageFile.getFileContent(this, ".scratchpad", mxUtils.bind(this, function(a) {
            null == a && (a = this.emptyLibraryXml);
            this.loadLibrary(new StorageLibrary(this, a, ".scratchpad"))
        })) : this.closeLibrary(this.scratchpad))
    };
    EditorUi.prototype.createLibraryDataFromImages = function(a) {
        var d = mxUtils.createXmlDocument(),
            b = d.createElement("mxlibrary");
        mxUtils.setTextContent(b, JSON.stringify(a));
        d.appendChild(b);
        return mxUtils.getXml(d)
    };
    EditorUi.prototype.closeLibrary = function(a) {
        null != a && (this.removeLibrarySidebar(a.getHash()), a.constructor != LocalLibrary && mxSettings.removeCustomLibrary(a.getHash()), ".scratchpad" == a.title && (this.scratchpad = null))
    };
    EditorUi.prototype.removeLibrarySidebar = function(a) {
        var d = this.sidebar.palettes[a];
        if (null != d) {
            for (var b = 0; b < d.length; b++) d[b].parentNode.removeChild(d[b]);
            delete this.sidebar.palettes[a]
        }
    };
    EditorUi.prototype.repositionLibrary = function(a) {
        var d = this.sidebar.container;
        if (null == a) {
            var b = this.sidebar.palettes["L.scratchpad"];
            null == b && (b = this.sidebar.palettes.search);
            null != b && (a = b[b.length - 1].nextSibling)
        }
        a = null != a ? a : d.firstChild.nextSibling.nextSibling;
        var b = d.lastChild,
            c = b.previousSibling;
        d.insertBefore(b, a);
        d.insertBefore(c, b)
    };
    EditorUi.prototype.loadLibrary = function(a, b) {
        var d = mxUtils.parseXml(a.getData());
        if ("mxlibrary" == d.documentElement.nodeName) {
            var c = JSON.parse(mxUtils.getTextContent(d.documentElement));
            this.libraryLoaded(a, c, d.documentElement.getAttribute("title"), b)
        } else throw {
            message: mxResources.get("notALibraryFile")
        };
    };
    EditorUi.prototype.getLibraryStorageHint = function(a) {
        return ""
    };
    EditorUi.prototype.libraryLoaded = function(a, b, c, e) {
        if (null != this.sidebar) {
            a.constructor != LocalLibrary && mxSettings.addCustomLibrary(a.getHash());
            ".scratchpad" == a.title && (this.scratchpad = a);
            var d = this.sidebar.palettes[a.getHash()],
                d = null != d ? d[d.length - 1].nextSibling : null;
            this.removeLibrarySidebar(a.getHash());
            var g = null,
                f = mxUtils.bind(this, function(b, d) {
                    0 == b.length && a.isEditable() ? (null == g && (g = document.createElement("div"), g.className = "geDropTarget", mxUtils.write(g, mxResources.get("dragElementsHere"))), d.appendChild(g)) : this.addLibraryEntries(b, d)
                });
            null != this.sidebar && null != b && this.sidebar.addEntries(b);
            c = null != c && 0 < c.length ? c : a.getTitle();
            var m = this.sidebar.addPalette(a.getHash(), c, null != e ? e : !0, mxUtils.bind(this, function(a) {
                f(b, a)
            }));
            this.repositionLibrary(d);
            var k = m.parentNode.previousSibling;
            e = k.getAttribute("title");
            null != e && 0 < e.length && ".scratchpad" != a.title && k.setAttribute("title", this.getLibraryStorageHint(a) + "\n" + e);
            var n = document.createElement("div");
            n.style.position = "absolute";
            n.style.right = "0px";
            n.style.top = "0px";
            n.style.padding = "8px";
            mxClient.IS_QUIRKS || 8 == document.documentMode || (n.style.backgroundColor = "inherit");
            k.style.position = "relative";
            var l = document.createElement("img");
            l.setAttribute("src", Dialog.prototype.closeImage);
            l.setAttribute("title", mxResources.get("close"));
            l.setAttribute("valign", "absmiddle");
            l.setAttribute("border", "0");
            l.style.cursor = "pointer";
            l.style.margin = "0 3px";
            var A = null;
            if (".scratchpad" != a.title || this.closableScratchpad) n.appendChild(l), mxEvent.addListener(l, "click", mxUtils.bind(this, function(b) {
                if (!mxEvent.isConsumed(b)) {
                    var d = mxUtils.bind(this, function() {
                        this.closeLibrary(a)
                    });
                    null != A ? this.confirm(mxResources.get("allChangesLost"), null, d, mxResources.get("cancel"), mxResources.get("discardChanges")) : d();
                    mxEvent.consume(b)
                }
            }));
            if (a.isEditable()) {
                var z = this.editor.graph,
                    B = null,
                    C = mxUtils.bind(this, function(d) {
                        this.showLibraryDialog(a.getTitle(), m, b, a, a.getMode());
                        mxEvent.consume(d)
                    }),
                    D = mxUtils.bind(this, function(d) {
                        a.setModified(!0);
                        a.isAutosave() ? (null != B && null != B.parentNode && B.parentNode.removeChild(B), B = l.cloneNode(!1), B.setAttribute("src", Editor.spinImage), B.setAttribute("title", mxResources.get("saving")), B.style.cursor = "default", B.style.marginRight = "2px", B.style.marginTop = "-2px", n.insertBefore(B, n.firstChild), k.style.paddingRight = 18 * n.childNodes.length + "px", this.saveLibrary(a.getTitle(), b, a, a.getMode(), !0, !0, function() {
                            null != B && null != B.parentNode && (B.parentNode.removeChild(B), k.style.paddingRight = 18 * n.childNodes.length + "px")
                        })) : null == A && (A = l.cloneNode(!1), A.setAttribute("src", IMAGE_PATH + "/download.png"), A.setAttribute("title", mxResources.get("save")), n.insertBefore(A, n.firstChild), mxEvent.addListener(A, "click", mxUtils.bind(this, function(d) {
                            this.saveLibrary(a.getTitle(), b, a, a.getMode(), a.constructor == LocalLibrary, !0, function() {
                                null == A || a.isModified() || (k.style.paddingRight = 18 * n.childNodes.length + "px", A.parentNode.removeChild(A), A = null)
                            });
                            mxEvent.consume(d)
                        })), k.style.paddingRight = 18 * n.childNodes.length + "px")
                    }),
                    E = mxUtils.bind(this, function(a, d, c, e) {
                        a = z.cloneCells(mxUtils.sortCells(z.model.getTopmostCells(a)));
                        for (var f = 0; f < a.length; f++) {
                            var k = z.getCellGeometry(a[f]);
                            null != k && k.translate(-d.x, -d.y)
                        }
                        m.appendChild(this.sidebar.createVertexTemplateFromCells(a, d.width, d.height, e || "", !0, !1, !1));
                        a = {
                            xml: Graph.compress(mxUtils.getXml(this.editor.graph.encodeCells(a))),
                            w: d.width,
                            h: d.height
                        };
                        null != e && (a.title = e);
                        b.push(a);
                        D(c);
                        null != g && null != g.parentNode && 0 < b.length && (g.parentNode.removeChild(g), g = null)
                    }),
                    F = mxUtils.bind(this, function(a) {
                        if (z.isSelectionEmpty()) z.getRubberband().isActive() ? (z.getRubberband().execute(a), z.getRubberband().reset()) : this.showError(mxResources.get("error"), mxResources.get("nothingIsSelected"), mxResources.get("ok"));
                        else {
                            var b = z.getSelectionCells(),
                                d = z.view.getBounds(b),
                                c = z.view.scale;
                            d.x /= c;
                            d.y /= c;
                            d.width /= c;
                            d.height /= c;
                            d.x -= z.view.translate.x;
                            d.y -= z.view.translate.y;
                            E(b, d)
                        }
                        mxEvent.consume(a)
                    });
                mxEvent.addGestureListeners(m, function() {}, mxUtils.bind(this, function(a) {
                    z.isMouseDown && null != z.panningManager && null != z.graphHandler.first && (z.graphHandler.suspend(), null != z.graphHandler.hint && (z.graphHandler.hint.style.visibility = "hidden"), m.style.backgroundColor = "#f1f3f4", m.style.cursor = "copy", z.panningManager.stop(), z.autoScroll = !1, mxEvent.consume(a))
                }), mxUtils.bind(this, function(a) {
                    z.isMouseDown && null != z.panningManager && null != z.graphHandler && (m.style.backgroundColor = "", m.style.cursor = "default", this.sidebar.showTooltips = !0, z.panningManager.stop(), z.graphHandler.reset(), z.isMouseDown = !1, z.autoScroll = !0, F(a), mxEvent.consume(a))
                }));
                mxEvent.addListener(m, "mouseleave", mxUtils.bind(this, function(a) {
                    z.isMouseDown && null != z.graphHandler.first && (z.graphHandler.resume(), null != z.graphHandler.hint && (z.graphHandler.hint.style.visibility = "visible"), m.style.backgroundColor = "", m.style.cursor = "", z.autoScroll = !0)
                }));
                Graph.fileSupport && (mxEvent.addListener(m, "dragover", mxUtils.bind(this, function(a) {
                    m.style.backgroundColor = "#f1f3f4";
                    a.dataTransfer.dropEffect = "copy";
                    m.style.cursor = "copy";
                    this.sidebar.hideTooltip();
                    a.stopPropagation();
                    a.preventDefault()
                })), mxEvent.addListener(m, "drop", mxUtils.bind(this, function(a) {
                    m.style.cursor = "";
                    m.style.backgroundColor = "";
                    0 < a.dataTransfer.files.length && this.importFiles(a.dataTransfer.files, 0, 0, this.maxImageSize, mxUtils.bind(this, function(d, c, e, k, n, p, l, z, t) {
                        if (null != d && "image/" == c.substring(0, 6)) d = "shape=image;verticalLabelPosition=bottom;verticalAlign=top;imageAspect=0;aspect=fixed;image=" + this.convertDataUri(d), d = [new mxCell("", new mxGeometry(0, 0, n, p), d)], d[0].vertex = !0, E(d, new mxRectangle(0, 0, n, p), a, mxEvent.isAltDown(a) ? null : l.substring(0, l.lastIndexOf(".")).replace(/_/g, " ")), null != g && null != g.parentNode && 0 < b.length && (g.parentNode.removeChild(g), g = null);
                        else {
                            var u = !1,
                                q = mxUtils.bind(this, function(d, c) {
                                    if (null != d && "application/pdf" == c) {
                                        var e = Editor.extractGraphModelFromPdf(d);
                                        null != e && 0 < e.length && (d = e)
                                    }
                                    if (null != d)
                                        if (e = mxUtils.parseXml(d), "mxlibrary" == e.documentElement.nodeName) try {
                                                var k = JSON.parse(mxUtils.getTextContent(e.documentElement));
                                                f(k, m);
                                                b = b.concat(k);
                                                D(a);
                                                this.spinner.stop();
                                                u = !0
                                            } catch (M) {} else if ("mxfile" == e.documentElement.nodeName) try {
                                                for (var n = e.documentElement.getElementsByTagName("diagram"), k = 0; k < n.length; k++) {
                                                    var p = this.stringToCells(Editor.getDiagramNodeXml(n[k])),
                                                        l = this.editor.graph.getBoundingBoxFromGeometry(p);
                                                    E(p, new mxRectangle(0, 0, l.width, l.height), a)
                                                }
                                                u = !0
                                            } catch (M) {
                                                null != window.console && console.log("error in drop handler:", M)
                                            }
                                    u || (this.spinner.stop(), this.handleError({
                                        message: mxResources.get("errorLoadingFile")
                                    }));
                                    null != g && null != g.parentNode && 0 < b.length && (g.parentNode.removeChild(g), g = null)
                                });
                            null != t && null != l && (/(\.v(dx|sdx?))($|\?)/i.test(l) || /(\.vs(x|sx?))($|\?)/i.test(l)) ? this.importVisio(t, function(a) {
                                q(a, "text/xml")
                            }, null, l) : !this.isOffline() && (new XMLHttpRequest).upload && this.isRemoteFileFormat(d, l) && null != t ? this.parseFile(t, mxUtils.bind(this, function(a) {
                                4 == a.readyState && (this.spinner.stop(), 200 <= a.status && 299 >= a.status ? q(a.responseText, "text/xml") : this.handleError({
                                    message: mxResources.get(413 == a.status ? "drawingTooLarge" : "invalidOrMissingFile")
                                }, mxResources.get("errorLoadingFile")))
                            })) : q(d, c)
                        }
                    }));
                    a.stopPropagation();
                    a.preventDefault()
                })), mxEvent.addListener(m, "dragleave", function(a) {
                    m.style.cursor = "";
                    m.style.backgroundColor = "";
                    a.stopPropagation();
                    a.preventDefault()
                }));
                l = l.cloneNode(!1);
                l.setAttribute("src", Editor.editImage);
                l.setAttribute("title", mxResources.get("edit"));
                n.insertBefore(l, n.firstChild);
                mxEvent.addListener(l, "click", C);
                mxEvent.addListener(m, "dblclick", function(a) {
                    mxEvent.getSource(a) == m && C(a)
                });
                e = l.cloneNode(!1);
                e.setAttribute("src", Editor.plusImage);
                e.setAttribute("title", mxResources.get("add"));
                n.insertBefore(e, n.firstChild);
                mxEvent.addListener(e, "click", F);
                this.isOffline() || ".scratchpad" != a.title || null == EditorUi.scratchpadHelpLink || (e = document.createElement("span"), e.setAttribute("title", mxResources.get("help")), e.style.cssText = "color:#a3a3a3;text-decoration:none;margin-right:2px;", mxUtils.write(e, "?"), mxEvent.addGestureListeners(e, mxUtils.bind(this, function(a) {
                    this.openLink(EditorUi.scratchpadHelpLink);
                    mxEvent.consume(a)
                })), n.insertBefore(e, n.firstChild))
            }
            k.appendChild(n);
            k.style.paddingRight = 18 * n.childNodes.length + "px"
        }
    };
    EditorUi.prototype.addLibraryEntries = function(a, b) {
        for (var d = 0; d < a.length; d++) {
            var c = a[d],
                e = c.data;
            if (null != e) {
                var e = this.convertDataUri(e),
                    g = "shape=image;verticalLabelPosition=bottom;verticalAlign=top;imageAspect=0;";
                "fixed" == c.aspect && (g += "aspect=fixed;");
                b.appendChild(this.sidebar.createVertexTemplate(g + "image=" + e, c.w, c.h, "", c.title || "", !1, !1, !0))
            } else null != c.xml && (e = this.stringToCells(Graph.decompress(c.xml)), 0 < e.length && b.appendChild(this.sidebar.createVertexTemplateFromCells(e, c.w, c.h, c.title || "", !0, !1, !0)))
        }
    };
    EditorUi.prototype.getResource = function(a) {
        return null != a ? a[mxLanguage] || a.main : null
    };
    EditorUi.prototype.footerHeight = 0;
    "1" == urlParams.savesidebar && (Sidebar.prototype.thumbWidth = 64, Sidebar.prototype.thumbHeight = 64);
    EditorUi.initTheme = function() {
        "atlas" == uiTheme ? (mxClient.link("stylesheet", STYLE_PATH + "/atlas.css"), "undefined" !== typeof Toolbar && (Toolbar.prototype.unselectedBackground = mxClient.IS_QUIRKS ? "none" : "linear-gradient(rgb(255, 255, 255) 0px, rgb(242, 242, 242) 100%)", Toolbar.prototype.selectedBackground = "rgb(242, 242, 242)"), Editor.prototype.initialTopSpacing = 3, EditorUi.prototype.menubarHeight = 41, EditorUi.prototype.toolbarHeight = 38) : "dark" == uiTheme && (mxClient.link("stylesheet", STYLE_PATH + "/dark.css"), Dialog.backdropColor = "#2a2a2a", Graph.prototype.defaultThemeName = "darkTheme", Graph.prototype.defaultPageBackgroundColor = "#2a2a2a", Graph.prototype.defaultPageBorderColor = "#505759", Format.prototype.inactiveTabBackgroundColor = "black", BaseFormatPanel.prototype.buttonBackgroundColor = "#2a2a2a", Sidebar.prototype.dragPreviewBorder = "1px dashed #cccccc", mxGraphHandler.prototype.previewColor = "#cccccc", StyleFormatPanel.prototype.defaultStrokeColor = "#cccccc", mxConstants.DROP_TARGET_COLOR = "#00ff00", mxClient.IS_SVG && (Editor.helpImage = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAP1BMVEUAAAD///////////////////////////////////////////////////////////////////////////////9Du/pqAAAAFXRSTlMAT30qCJRBboyDZyCgRzUUdF46MJlgXETgAAAAeklEQVQY022O2w4DIQhEQUURda/9/28tUO2+7CQS5sgQ4F1RapX78YUwRqQjTU8ILqQfKerTKTvACJ4nLX3krt+8aS82oI8aQC4KavRgtvEW/mDvsICgA03PSGRr79MqX1YPNIxzjyqtw8ZnnRo4t5a5undtJYRywau+ds4Cyza3E6YAAAAASUVORK5CYII=", Editor.checkmarkImage = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABUAAAAVCAMAAACeyVWkAAAARVBMVEUAAACZmZkICAgEBASNjY2Dg4MYGBiTk5N5eXl1dXVmZmZQUFBCQkI3NzceHh4MDAykpKSJiYl+fn5sbGxaWlo/Pz8SEhK96uPlAAAAAXRSTlMAQObYZgAAAE5JREFUGNPFzTcSgDAQQ1HJGUfy/Y9K7V1qeOUfzQifCQZai1XHaz11LFysbDbzgDSSWMZiETz3+b8yNUc/MMsktxuC8XQBSncdLwz+8gCCggGXzBcozAAAAABJRU5ErkJggg=="))
    };
    EditorUi.initTheme();
    EditorUi.prototype.showImageDialog = function(a, b, c, e, f) {
        a = new ImageDialog(this, a, b, c, e, f);
        this.showDialog(a.container, Graph.fileSupport ? 480 : 360, Graph.fileSupport ? 200 : 90, !0, !0);
        a.init()
    };
    EditorUi.prototype.showBackgroundImageDialog = function(a, b) {
        a = null != a ? a : mxUtils.bind(this, function(a, b) {
            if (!b) {
                var d = new ChangePageSetup(this, null, a);
                d.ignoreColor = !0;
                this.editor.graph.model.execute(d)
            }
        });
        var d = new BackgroundImageDialog(this, a, b);
        this.showDialog(d.container, 360, 200, !0, !0);
        d.init()
    };
    EditorUi.prototype.showLibraryDialog = function(a, b, c, e, f) {
        a = new LibraryDialog(this, a, b, c, e, f);
        this.showDialog(a.container, 640, 440, !0, !1, mxUtils.bind(this, function(a) {
            a && null == this.getCurrentFile() && "1" != urlParams.embed && this.showSplash()
        }));
        a.init()
    };
    var e = EditorUi.prototype.createFormat;
    EditorUi.prototype.createFormat = function(a) {
        var b = e.apply(this, arguments);
        this.editor.graph.addListener("viewStateChanged", mxUtils.bind(this, function(a) {
            this.editor.graph.isSelectionEmpty() && b.refresh()
        }));
        return b
    };
    EditorUi.prototype.createSidebarFooterContainer = function() {
        var a = this.createDiv("geSidebarContainer geSidebarFooter");
        a.style.position = "absolute";
        a.style.overflow = "hidden";
        var b = document.createElement("a");
        b.className = "geTitle";
        b.style.color = "#DF6C0C";
        b.style.fontWeight = "bold";
        b.style.height = "100%";
        b.style.paddingTop = "9px";
        b.innerHTML = '<span style="font-size:18px;margin-right:5px;">+</span>';
        mxUtils.write(b, mxResources.get("moreShapes") + "...");
        mxEvent.addListener(b, mxClient.IS_POINTER ? "pointerdown" : "mousedown", mxUtils.bind(this, function(a) {
            a.preventDefault()
        }));
        mxEvent.addListener(b, "click", mxUtils.bind(this, function(a) {
            this.actions.get("shapes").funct();
            mxEvent.consume(a)
        }));
        a.appendChild(b);
        return a
    };
    EditorUi.prototype.handleError = function(a, b, c, e, f, k, l) {
        var d = null != this.spinner && null != this.spinner.pause ? this.spinner.pause() : function() {},
            g = null != a && null != a.error ? a.error : a;
        if (null != a && null != a.stack && null != a.message) try {
            l ? null != window.console && console.error("EditorUi.handleError:", a) : EditorUi.logError("Caught: " + (null != a.message ? a.message : "null"), null, null, null, a, "INFO")
        } catch (B) {}
        if (null != g || null != b) {
            l = mxUtils.htmlEntities(mxResources.get("unknownError"));
            var m = mxResources.get("ok"),
                n = null;
            b = null != b ? b : mxResources.get("error");
            if (null != g) {
                null != g.retry && (m = mxResources.get("cancel"), n = function() {
                    d();
                    g.retry()
                });
                if (404 == g.code || 404 == g.status || 403 == g.code) {
                    l = 403 == g.code ? null != g.message ? mxUtils.htmlEntities(g.message) : mxUtils.htmlEntities(mxResources.get("accessDenied")) : null != f ? f : mxUtils.htmlEntities(mxResources.get("fileNotFoundOrDenied") + (null != this.drive && null != this.drive.user ? " (" + this.drive.user.displayName + ", " + this.drive.user.email + ")" : ""));
                    var p = null != k ? k : window.location.hash;
                    if (null != p && ("#G" == p.substring(0, 2) || "#Uhttps%3A%2F%2Fdrive.google.com%2Fuc%3Fid%3D" == p.substring(0, 45)) && (null != a && null != a.error && (null != a.error.errors && 0 < a.error.errors.length && "fileAccess" == a.error.errors[0].reason || null != a.error.data && 0 < a.error.data.length && "fileAccess" == a.error.data[0].reason) || 404 == g.code || 404 == g.status)) {
                        p = "#U" == p.substring(0, 2) ? p.substring(45, p.lastIndexOf("%26ex")) : p.substring(2);
                        this.showError(b, l, mxResources.get("openInNewWindow"), mxUtils.bind(this, function() {
                            this.editor.graph.openLink("https://drive.google.com/open?id=" + p);
                            this.handleError(a, b, c, e, f)
                        }), n, mxResources.get("changeUser"), mxUtils.bind(this, function() {
                            function a() {
                                e.innerHTML = "";
                                for (var a = 0; a < b.length; a++) {
                                    var d = document.createElement("option");
                                    mxUtils.write(d, b[a].displayName);
                                    d.value = a;
                                    e.appendChild(d);
                                    d = document.createElement("option");
                                    d.innerHTML = "&nbsp;&nbsp;&nbsp;";
                                    mxUtils.write(d, "<" + b[a].email + ">");
                                    d.setAttribute("disabled", "disabled");
                                    e.appendChild(d)
                                }
                                d = document.createElement("option");
                                mxUtils.write(d, mxResources.get("addAccount"));
                                d.value = b.length;
                                e.appendChild(d)
                            }
                            var b = this.drive.getUsersList(),
                                d = document.createElement("div"),
                                c = document.createElement("span");
                            c.style.marginTop = "6px";
                            mxUtils.write(c, mxResources.get("changeUser") + ": ");
                            d.appendChild(c);
                            var e = document.createElement("select");
                            e.style.width = "200px";
                            a();
                            mxEvent.addListener(e, "change", mxUtils.bind(this, function() {
                                var d = e.value,
                                    c = b.length != d;
                                c && this.drive.setUser(b[d]);
                                this.drive.authorize(c, mxUtils.bind(this, function() {
                                    c || (b = this.drive.getUsersList(), a())
                                }), mxUtils.bind(this, function(a) {
                                    this.handleError(a)
                                }), !0)
                            }));
                            d.appendChild(e);
                            d = new CustomDialog(this, d, mxUtils.bind(this, function() {
                                this.loadFile(window.location.hash.substr(1), !0)
                            }));
                            this.showDialog(d.container, 300, 75, !0, !0)
                        }), mxResources.get("cancel"), mxUtils.bind(this, function() {
                            this.hideDialog();
                            null != c && c()
                        }), 480, 150);
                        return
                    }
                }
                null != g.message ? l = mxUtils.htmlEntities(g.message) : null != g.response && null != g.response.error ? l = mxUtils.htmlEntities(g.response.error) : "undefined" !== typeof window.App && (g.code == App.ERROR_TIMEOUT ? l = mxUtils.htmlEntities(mxResources.get("timeout")) : g.code == App.ERROR_BUSY && (l = mxUtils.htmlEntities(mxResources.get("busy"))))
            }
            var z = k = null;
            null != g && null != g.helpLink && (k = mxResources.get("help"), z = mxUtils.bind(this, function() {
                return this.editor.graph.openLink(g.helpLink)
            }));
            this.showError(b, l, m, c, n, null, null, k, z, null, null, null, e ? c : null)
        } else null != c && c()
    };
    EditorUi.prototype.alert = function(a, b, c) {
        a = new ErrorDialog(this, null, a, mxResources.get("ok"), b);
        this.showDialog(a.container, c || 340, 100, !0, !1);
        a.init()
    };
    EditorUi.prototype.confirm = function(a, b, c, e, f, k) {
        var d = null != this.spinner && null != this.spinner.pause ? this.spinner.pause() : function() {},
            g = Math.min(200, 28 * Math.ceil(a.length / 50));
        a = new ConfirmDialog(this, a, function() {
            d();
            null != b && b()
        }, function() {
            d();
            null != c && c()
        }, e, f, null, null, null, null, g);
        this.showDialog(a.container, 340, 46 + g, !0, k);
        a.init()
    };
    EditorUi.prototype.showBanner = function(a, b, c) {
        var d = !1;
        if (!(this.bannerShowing || this["hideBanner" + a] || isLocalStorage && null != mxSettings.settings && null != mxSettings.settings["close" + a])) {
            var e = document.createElement("div");
            e.style.cssText = "position:absolute;bottom:10px;left:50%;max-width:90%;padding:18px 34px 12px 20px;font-size:16px;font-weight:bold;white-space:nowrap;cursor:pointer;z-index:" + mxPopupMenu.prototype.zIndex + ";";
            mxUtils.setPrefixedStyle(e.style, "box-shadow", "1px 1px 2px 0px #ddd");
            mxUtils.setPrefixedStyle(e.style, "transform", "translate(-50%,120%)");
            mxUtils.setPrefixedStyle(e.style, "transition", "all 1s ease");
            e.className = "geBtn gePrimaryBtn";
            d = document.createElement("img");
            d.setAttribute("src", IMAGE_PATH + "/logo.png");
            d.setAttribute("border", "0");
            d.setAttribute("align", "absmiddle");
            d.style.cssText = "margin-top:-4px;margin-left:8px;margin-right:12px;width:26px;height:26px;";
            e.appendChild(d);
            d = document.createElement("img");
            d.setAttribute("src", Dialog.prototype.closeImage);
            d.setAttribute("title", mxResources.get("close"));
            d.setAttribute("border", "0");
            d.style.cssText = "position:absolute;right:10px;top:12px;filter:invert(1);padding:6px;margin:-6px;cursor:default;";
            e.appendChild(d);
            mxUtils.write(e, b);
            document.body.appendChild(e);
            this.bannerShowing = !0;
            var g = document.createElement("div");
            g.style.cssText = "font-size:11px;text-align:center;font-weight:normal;";
            var f = document.createElement("input");
            f.setAttribute("type", "checkbox");
            f.setAttribute("id", "geDoNotShowAgainCheckbox");
            f.style.marginRight = "6px";
            g.appendChild(f);
            b = document.createElement("label");
            b.setAttribute("for", "geDoNotShowAgainCheckbox");
            mxUtils.write(b, mxResources.get("doNotShowAgain"));
            g.appendChild(b);
            e.style.paddingBottom = "30px";
            e.appendChild(g);
            var m = mxUtils.bind(this, function() {
                null != e.parentNode && (e.parentNode.removeChild(e), this.bannerShowing = !1, f.checked && (this["hideBanner" + a] = !0, isLocalStorage && null != mxSettings.settings && (mxSettings.settings["close" + a] = Date.now(), mxSettings.save())))
            });
            mxEvent.addListener(d, "click", mxUtils.bind(this, function(a) {
                mxEvent.consume(a);
                m()
            }));
            var k = mxUtils.bind(this, function() {
                mxUtils.setPrefixedStyle(e.style, "transform", "translate(-50%,120%)");
                window.setTimeout(mxUtils.bind(this, function() {
                    m()
                }), 1E3)
            });
            mxEvent.addListener(e, "click", mxUtils.bind(this, function(a) {
                var d = mxEvent.getSource(a);
                d != f && d != b ? (null != c && c(), m(), mxEvent.consume(a)) : k()
            }));
            window.setTimeout(mxUtils.bind(this, function() {
                mxUtils.setPrefixedStyle(e.style, "transform", "translate(-50%,0%)")
            }), 500);
            window.setTimeout(k, 3E4);
            d = !0
        }
        return d
    };
    EditorUi.prototype.setCurrentFile = function(a) {
        null != a && (a.opened = new Date);
        this.currentFile = a
    };
    EditorUi.prototype.getCurrentFile = function() {
        return this.currentFile
    };
    EditorUi.prototype.isExportToCanvas = function() {
        return this.editor.isExportToCanvas()
    };
    EditorUi.prototype.createImageDataUri = function(a, b, c, e) {
        var d = a.toDataURL("image/" + c);
        if (6 >= d.length || d == a.cloneNode(!1).toDataURL("image/" + c)) throw {
            message: "Invalid image"
        };
        null != b && (d = Editor.writeGraphModelToPng(d, "tEXt", "mxfile", encodeURIComponent(b)));
        0 < e && (d = Editor.writeGraphModelToPng(d, "pHYs", "dpi", e));
        return d
    };
    EditorUi.prototype.saveCanvas = function(a, b, c, e, f) {
        var d = "jpeg" == c ? "jpg" : c;
        e = this.getBaseFilename(e) + "." + d;
        a = this.createImageDataUri(a, b, c, f);
        this.saveData(e, d, a.substring(a.lastIndexOf(",") + 1), "image/" + c, !0)
    };
    EditorUi.prototype.isLocalFileSave = function() {
        return "remote" != urlParams.save && (mxClient.IS_IE || "undefined" !== typeof window.Blob && "undefined" !== typeof window.URL) && 9 != document.documentMode && 8 != document.documentMode && 7 != document.documentMode && !mxClient.IS_QUIRKS || this.isOfflineApp() || mxClient.IS_IOS
    };
    EditorUi.prototype.showTextDialog = function(a, b) {
        var d = new TextareaDialog(this, a, b, null, null, mxResources.get("close"));
        d.textarea.style.width = "600px";
        d.textarea.style.height = "380px";
        this.showDialog(d.container, 620, 460, !0, !0, null, null, null, null, !0);
        d.init();
        document.execCommand("selectall", !1, null)
    };
    EditorUi.prototype.doSaveLocalFile = function(a, b, c, e, f, k) {
        "text/xml" != c || /(\.drawio)$/i.test(b) || /(\.xml)$/i.test(b) || (b = b + "." + (null != k ? k : "drawio"));
        if (window.Blob && navigator.msSaveOrOpenBlob) a = e ? this.base64ToBlob(a, c) : new Blob([a], {
            type: c
        }), navigator.msSaveOrOpenBlob(a, b);
        else if (mxClient.IS_IE) c = window.open("about:blank", "_blank"), null == c ? mxUtils.popup(a, !0) : (c.document.write(a), c.document.close(), c.document.execCommand("SaveAs", !0, b), c.close());
        else if (mxClient.IS_IOS && this.isOffline()) navigator.standalone || null == c || "image/" != c.substring(0, 6) ? this.showTextDialog(b + ":", a) : this.openInNewWindow(a, c, e);
        else {
            var d = document.createElement("a");
            k = (null == navigator.userAgent || 0 > navigator.userAgent.indexOf("PaleMoon/")) && "undefined" !== typeof d.download;
            if (mxClient.IS_GC && null != navigator.userAgent) {
                var g = navigator.userAgent.match(/Chrom(e|ium)\/([0-9]+)\./);
                k = 65 == (g ? parseInt(g[2], 10) : !1) ? !1 : k
            }
            if (k || this.isOffline()) {
                d.href = URL.createObjectURL(e ? this.base64ToBlob(a, c) : new Blob([a], {
                    type: c
                }));
                k ? d.download = b : d.setAttribute("target", "_blank");
                document.body.appendChild(d);
                try {
                    window.setTimeout(function() {
                        URL.revokeObjectURL(d.href)
                    }, 2E4), d.click(), d.parentNode.removeChild(d)
                } catch (v) {}
            } else this.createEchoRequest(a, b, c, e, f).simulate(document, "_blank")
        }
    };
    EditorUi.prototype.createEchoRequest = function(a, b, c, e, f, k) {
        a = "xml=" + encodeURIComponent(a);
        return new mxXmlRequest(SAVE_URL, a + (null != c ? "&mime=" + c : "") + (null != f ? "&format=" + f : "") + (null != k ? "&base64=" + k : "") + (null != b ? "&filename=" + encodeURIComponent(b) : "") + (e ? "&binary=1" : ""))
    };
    EditorUi.prototype.base64ToBlob = function(a, b) {
        b = b || "";
        for (var d = atob(a), c = d.length, e = Math.ceil(c / 1024), g = Array(e), f = 0; f < e; ++f) {
            for (var k = 1024 * f, l = Math.min(k + 1024, c), x = Array(l - k), y = 0; k < l; ++y, ++k) x[y] = d[k].charCodeAt(0);
            g[f] = new Uint8Array(x)
        }
        return new Blob(g, {
            type: b
        })
    };
    EditorUi.prototype.saveLocalFile = function(a, b, c, e, f, k, l, q) {
        k = null != k ? k : !1;
        l = null != l ? l : "vsdx" != f && (!mxClient.IS_IOS || !navigator.standalone);
        f = this.getServiceCount(k);
        isLocalStorage && f++;
        var d = 4 >= f ? 2 : 6 < f ? 4 : 3;
        b = new CreateDialog(this, b, mxUtils.bind(this, function(b, d) {
            try {
                if ("_blank" == d)
                    if (null != c && "image/" == c.substring(0, 6)) this.openInNewWindow(a, c, e);
                    else {
                        var g = window.open("about:blank");
                        null == g ? mxUtils.popup(a, !0) : (g.document.write("<pre>" + mxUtils.htmlEntities(a, !1) + "</pre>"), g.document.close())
                    }
                else d == App.MODE_DEVICE || "download" == d ? this.doSaveLocalFile(a, b, c, e, null, q) : null != b && 0 < b.length && this.pickFolder(d, mxUtils.bind(this, function(g) {
                    try {
                        this.exportFile(a, b, c, e, d, g)
                    } catch (B) {
                        this.handleError(B)
                    }
                }))
            } catch (z) {
                this.handleError(z)
            }
        }), mxUtils.bind(this, function() {
            this.hideDialog()
        }), mxResources.get("saveAs"), mxResources.get("download"), !1, k, l, null, 1 < f, d, a, c, e);
        k = this.isServices(f) ? f > d ? 390 : 270 : 160;
        this.showDialog(b.container, 400, k, !0, !0);
        b.init()
    };
    EditorUi.prototype.openInNewWindow = function(a, b, c) {
        var d = window.open("about:blank");
        null == d || null == d.document ? mxUtils.popup(a, !0) : ("image/svg+xml" != b || mxClient.IS_SVG ? "image/svg+xml" == b ? d.document.write("<html>" + a + "</html>") : (a = c ? a : btoa(unescape(encodeURIComponent(a))), d.document.write('<html><img style="max-width:100%;" src="data:' + b + ";base64," + a + '"/></html>')) : d.document.write("<html><pre>" + mxUtils.htmlEntities(a, !1) + "</pre></html>"), d.document.close())
    };
    var c = EditorUi.prototype.addChromelessToolbarItems;
    EditorUi.prototype.addChromelessToolbarItems = function(a) {
        if (this.isExportToCanvas()) {
            this.exportDialog = null;
            var b = a(mxUtils.bind(this, function(a) {
                var d = mxUtils.bind(this, function() {
                    mxEvent.removeListener(this.editor.graph.container, "click", d);
                    null != this.exportDialog && (this.exportDialog.parentNode.removeChild(this.exportDialog), this.exportDialog = null)
                });
                if (null != this.exportDialog) d.apply(this);
                else {
                    this.exportDialog = document.createElement("div");
                    var c = b.getBoundingClientRect();
                    mxUtils.setPrefixedStyle(this.exportDialog.style, "borderRadius", "5px");
                    this.exportDialog.style.position = "fixed";
                    this.exportDialog.style.textAlign = "center";
                    this.exportDialog.style.fontFamily = "Helvetica,Arial";
                    this.exportDialog.style.backgroundColor = "#000000";
                    this.exportDialog.style.width = "50px";
                    this.exportDialog.style.height = "50px";
                    this.exportDialog.style.padding = "4px 2px 4px 2px";
                    this.exportDialog.style.color = "#ffffff";
                    mxUtils.setOpacity(this.exportDialog, 70);
                    this.exportDialog.style.left = c.left + "px";
                    this.exportDialog.style.bottom = parseInt(this.chromelessToolbar.style.bottom) + this.chromelessToolbar.offsetHeight + 4 + "px";
                    c = mxUtils.getCurrentStyle(this.editor.graph.container);
                    this.exportDialog.style.zIndex = c.zIndex;
                    var e = new Spinner({
                        lines: 8,
                        length: 6,
                        width: 5,
                        radius: 6,
                        rotate: 0,
                        color: "#fff",
                        speed: 1.5,
                        trail: 60,
                        shadow: !1,
                        hwaccel: !1,
                        top: "28px",
                        zIndex: 2E9
                    });
                    e.spin(this.exportDialog);
                    this.editor.exportToCanvas(mxUtils.bind(this, function(a) {
                        e.stop();
                        this.exportDialog.style.width = "auto";
                        this.exportDialog.style.height = "auto";
                        this.exportDialog.style.padding = "10px";
                        var b = this.createImageDataUri(a, null, "png");
                        a = document.createElement("img");
                        a.style.maxWidth = "140px";
                        a.style.maxHeight = "140px";
                        a.style.cursor = "pointer";
                        a.style.backgroundColor = "white";
                        a.setAttribute("title", mxResources.get("openInNewWindow"));
                        a.setAttribute("border", "0");
                        a.setAttribute("src", b);
                        this.exportDialog.appendChild(a);
                        mxEvent.addListener(a, "click", mxUtils.bind(this, function() {
                            this.openInNewWindow(b.substring(b.indexOf(",") + 1), "image/png", !0);
                            d.apply(this, arguments)
                        }))
                    }), null, this.thumbImageCache, null, mxUtils.bind(this, function(a) {
                        this.spinner.stop();
                        this.handleError(a)
                    }), null, null, null, null, null, null, null, Editor.defaultBorder);
                    mxEvent.addListener(this.editor.graph.container, "click", d);
                    document.body.appendChild(this.exportDialog)
                }
                mxEvent.consume(a)
            }), Editor.cameraLargeImage, mxResources.get("export"))
        }
        c.apply(this, arguments)
    };
    EditorUi.prototype.saveData = function(a, b, c, e, f) {
        this.isLocalFileSave() ? this.saveLocalFile(c, a, e, f, b) : this.saveRequest(a, b, mxUtils.bind(this, function(a, d) {
            return this.createEchoRequest(c, a, e, f, b, d)
        }), c, f, e)
    };
    EditorUi.prototype.saveRequest = function(a, b, c, e, f, k, l) {
        l = null != l ? l : !mxClient.IS_IOS || !navigator.standalone;
        var d = this.getServiceCount(!1);
        isLocalStorage && d++;
        var g = 4 >= d ? 2 : 6 < d ? 4 : 3;
        a = new CreateDialog(this, a, mxUtils.bind(this, function(a, d) {
            if ("_blank" == d || null != a && 0 < a.length) {
                var g = c("_blank" == d ? null : a, d == App.MODE_DEVICE || "download" == d || null == d || "_blank" == d ? "0" : "1");
                null != g && (d == App.MODE_DEVICE || "download" == d || "_blank" == d ? g.simulate(document, "_blank") : this.pickFolder(d, mxUtils.bind(this, function(c) {
                    k = null != k ? k : "pdf" == b ? "application/pdf" : "image/" + b;
                    if (null != e) try {
                        this.exportFile(e, a, k, !0, d, c)
                    } catch (B) {
                        this.handleError(B)
                    } else this.spinner.spin(document.body, mxResources.get("saving")) && g.send(mxUtils.bind(this, function() {
                        this.spinner.stop();
                        if (200 <= g.getStatus() && 299 >= g.getStatus()) try {
                            this.exportFile(g.getText(), a, k, !0, d, c)
                        } catch (B) {
                            this.handleError(B)
                        } else this.handleError({
                            message: mxResources.get("errorSavingFile")
                        })
                    }), function(a) {
                        this.spinner.stop();
                        this.handleError(a)
                    })
                })))
            }
        }), mxUtils.bind(this, function() {
            this.hideDialog()
        }), mxResources.get("saveAs"), mxResources.get("download"), !1, !1, l, null, 1 < d, g, e, k, f);
        d = this.isServices(d) ? 4 < d ? 390 : 270 : 160;
        this.showDialog(a.container, 380, d, !0, !0);
        a.init()
    };
    EditorUi.prototype.isServices = function(a) {
        return 1 != a
    };
    EditorUi.prototype.getEditBlankXml = function() {
        return this.getFileData(!0)
    };
    EditorUi.prototype.exportFile = function(a, b, c, e, f, k) {};
    EditorUi.prototype.pickFolder = function(a, b, c) {
        b(null)
    };
    EditorUi.prototype.exportSvg = function(a, b, c, e, f, k, l, q, v, x, y) {
        if (this.spinner.spin(document.body, mxResources.get("export"))) try {
            var d = this.editor.graph.isSelectionEmpty();
            c = null != c ? c : d;
            var g = b ? null : this.editor.graph.background;
            g == mxConstants.NONE && (g = null);
            null == g && 0 == b && (g = y ? this.editor.graph.defaultPageBackgroundColor : "#ffffff");
            var m = this.editor.graph.getSvg(g, a, l, q, null, c, null, null, "blank" == x ? "_blank" : "self" == x ? "_top" : null, null, !0, y);
            e && this.editor.graph.addSvgShadow(m);
            var n = this.getBaseFilename() + ".svg",
                p = mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    f && a.setAttribute("content", this.getFileData(!0, null, null, null, c, v, null, null, null, !1));
                    var b = '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n' + mxUtils.getXml(a);
                    this.isLocalFileSave() || b.length <= MAX_REQUEST_SIZE ? this.saveData(n, "svg", b, "image/svg+xml") : this.handleError({
                        message: mxResources.get("drawingTooLarge")
                    }, mxResources.get("error"), mxUtils.bind(this, function() {
                        mxUtils.popup(b)
                    }))
                });
            this.editor.addFontCss(m);
            this.editor.graph.mathEnabled && this.editor.addMathCss(m);
            k ? (null == this.thumbImageCache && (this.thumbImageCache = {}), this.editor.convertImages(m, p, this.thumbImageCache)) : p(m)
        } catch (E) {
            this.handleError(E)
        }
    };
    EditorUi.prototype.addRadiobox = function(a, b, c, e, f, k, l) {
        return this.addCheckbox(a, c, e, f, k, l, !0, b)
    };
    EditorUi.prototype.addCheckbox = function(a, b, c, e, f, k, l, q) {
        k = null != k ? k : !0;
        var d = document.createElement("input");
        d.style.marginRight = "8px";
        d.style.marginTop = "16px";
        d.setAttribute("type", l ? "radio" : "checkbox");
        l = "geCheckbox-" + Editor.guid();
        d.id = l;
        null != q && d.setAttribute("name", q);
        c && (d.setAttribute("checked", "checked"), d.defaultChecked = !0);
        e && d.setAttribute("disabled", "disabled");
        k && (a.appendChild(d), c = document.createElement("label"), mxUtils.write(c, b), c.setAttribute("for", l), a.appendChild(c), f || mxUtils.br(a));
        return d
    };
    EditorUi.prototype.addEditButton = function(a, b) {
        var d = this.addCheckbox(a, mxResources.get("edit") + ":", !0, null, !0);
        d.style.marginLeft = "24px";
        var c = this.getCurrentFile(),
            e = "";
        null != c && c.getMode() != App.MODE_DEVICE && c.getMode() != App.MODE_BROWSER && (e = window.location.href);
        var g = document.createElement("select");
        g.style.width = "120px";
        g.style.marginLeft = "8px";
        g.style.marginRight = "10px";
        g.className = "geBtn";
        c = document.createElement("option");
        c.setAttribute("value", "blank");
        mxUtils.write(c, mxResources.get("makeCopy"));
        g.appendChild(c);
        c = document.createElement("option");
        c.setAttribute("value", "custom");
        mxUtils.write(c, mxResources.get("custom") + "...");
        g.appendChild(c);
        a.appendChild(g);
        mxEvent.addListener(g, "change", mxUtils.bind(this, function() {
            if ("custom" == g.value) {
                var a = new FilenameDialog(this, e, mxResources.get("ok"), function(a) {
                    null != a ? e = a : g.value = "blank"
                }, mxResources.get("url"), null, null, null, null, function() {
                    g.value = "blank"
                });
                this.showDialog(a.container, 300, 80, !0, !1);
                a.init()
            }
        }));
        mxEvent.addListener(d, "change", mxUtils.bind(this, function() {
            d.checked && (null == b || b.checked) ? g.removeAttribute("disabled") : g.setAttribute("disabled", "disabled")
        }));
        mxUtils.br(a);
        return {
            getLink: function() {
                return d.checked ? "blank" === g.value ? "_blank" : e : null
            },
            getEditInput: function() {
                return d
            },
            getEditSelect: function() {
                return g
            }
        }
    };
    EditorUi.prototype.addLinkSection = function(a, b) {
        function d() {
            f.innerHTML = '<div style="width:100%;height:100%;box-sizing:border-box;' + (null != g && g != mxConstants.NONE ? "border:1px solid black;background-color:" + g : "background-position:center center;background-repeat:no-repeat;background-image:url('" + Dialog.prototype.closeImage + "')") + ';"></div>'
        }
        mxUtils.write(a, mxResources.get("links") + ":");
        var c = document.createElement("select");
        c.style.width = "100px";
        c.style.marginLeft = "8px";
        c.style.marginRight = "10px";
        c.className = "geBtn";
        var e = document.createElement("option");
        e.setAttribute("value", "auto");
        mxUtils.write(e, mxResources.get("automatic"));
        c.appendChild(e);
        e = document.createElement("option");
        e.setAttribute("value", "blank");
        mxUtils.write(e, mxResources.get("openInNewWindow"));
        c.appendChild(e);
        e = document.createElement("option");
        e.setAttribute("value", "self");
        mxUtils.write(e, mxResources.get("openInThisWindow"));
        c.appendChild(e);
        b && (e = document.createElement("option"), e.setAttribute("value", "frame"), mxUtils.write(e, mxResources.get("openInThisWindow") + " (" + mxResources.get("iframe") + ")"), c.appendChild(e));
        a.appendChild(c);
        mxUtils.write(a, mxResources.get("borderColor") + ":");
        var g = "#0000ff",
            f = null,
            f = mxUtils.button("", mxUtils.bind(this, function(a) {
                this.pickColor(g || "none", function(a) {
                    g = a;
                    d()
                });
                mxEvent.consume(a)
            }));
        d();
        f.style.padding = mxClient.IS_FF ? "4px 2px 4px 2px" : "4px";
        f.style.marginLeft = "4px";
        f.style.height = "22px";
        f.style.width = "22px";
        f.style.position = "relative";
        f.style.top = mxClient.IS_IE || mxClient.IS_IE11 || mxClient.IS_EDGE ? "6px" : "1px";
        f.className = "geColorBtn";
        a.appendChild(f);
        mxUtils.br(a);
        return {
            getColor: function() {
                return g
            },
            getTarget: function() {
                return c.value
            },
            focus: function() {
                c.focus()
            }
        }
    };
    EditorUi.prototype.createUrlParameters = function(a, b, c, e, f, k, l) {
        l = null != l ? l : [];
        e && ("1" == urlParams.dev && l.push("lightbox=1"), "auto" != a && l.push("target=" + a), null != b && b != mxConstants.NONE && l.push("highlight=" + ("#" == b.charAt(0) ? b.substring(1) : b)), null != f && 0 < f.length && l.push("edit=" + encodeURIComponent(f)), k && l.push("layers=1"), this.editor.graph.foldingEnabled && l.push("nav=1"));
        c && null != this.currentPage && null != this.pages && this.currentPage != this.pages[0] && l.push("page-id=" + this.currentPage.getId());
        return l
    };
    EditorUi.prototype.createLink = function(a, b, c, e, f, k, l, q, v) {
        v = null != v ? v : this.createUrlParameters(a, b, c, e, f, k);
        a = this.getCurrentFile();
        b = !0;
        null != l ? c = "#U" + encodeURIComponent(l) : (a = this.getCurrentFile(), q || null == a || a.constructor != window.DriveFile ? c = "#R" + encodeURIComponent(c ? this.getFileData(!0, null, null, null, null, null, null, !0, null, !1) : Graph.compress(mxUtils.getXml(this.editor.getGraphXml()))) : (c = "#" + a.getHash(), b = !1));
        b && null != a && null != a.getTitle() && a.getTitle() != this.defaultFilename && v.push("title=" + encodeURIComponent(a.getTitle()));
        return (e && "1" != urlParams.dev ? EditorUi.lightboxHost : mxClient.IS_CHROMEAPP || EditorUi.isElectronApp || !/.*\.draw\.io$/.test(window.location.hostname) ? EditorUi.drawHost : "https://" + window.location.host) + "/" + (0 < v.length ? "?" + v.join("&") : "") + c
    };
    EditorUi.prototype.createHtml = function(a, b, c, e, f, k, l, q, v, x, y) {
        this.getBasenames();
        var d = {};
        "" != f && f != mxConstants.NONE && (d.highlight = f);
        "auto" !== e && (d.target = e);
        v || (d.lightbox = !1);
        d.nav = this.editor.graph.foldingEnabled;
        c = parseInt(c);
        isNaN(c) || 100 == c || (d.zoom = c / 100);
        c = [];
        l && (c.push("pages"), d.resize = !0, null != this.pages && null != this.currentPage && (d.page = mxUtils.indexOf(this.pages, this.currentPage)));
        b && (c.push("zoom"), d.resize = !0);
        q && c.push("layers");
        0 < c.length && (v && c.push("lightbox"), d.toolbar = c.join(" "));
        null != x && 0 < x.length && (d.edit = x);
        null != a ? d.url = a : d.xml = this.getFileData(!0, null, null, null, null, !l);
        b = '<div class="mxgraph" style="' + (k ? "max-width:100%;" : "") + ("" != c ? "border:1px solid transparent;" : "") + '" data-mxgraph="' + mxUtils.htmlEntities(JSON.stringify(d)) + '"></div>';
        a = null != a ? "&fetch=" + encodeURIComponent(a) : "";
        y(b, '<script type="text/javascript" src="' + (0 < a.length ? ("1" == urlParams.dev ? "https://test.draw.io/embed2.js?dev=1" : EditorUi.drawHost + "/embed2.js?") + a : "1" == urlParams.dev ? "https://test.draw.io/js/viewer-static.min.js" : window.VIEWER_URL ? window.VIEWER_URL : EditorUi.drawHost + "/js/viewer-static.min.js") + '"></script>')
    };
    EditorUi.prototype.showHtmlDialog = function(a, b, c, e) {
        var d = document.createElement("div");
        d.style.whiteSpace = "nowrap";
        var g = document.createElement("h3");
        mxUtils.write(g, mxResources.get("html"));
        g.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:12px";
        d.appendChild(g);
        var f = document.createElement("div");
        f.style.cssText = "border-bottom:1px solid lightGray;padding-bottom:8px;margin-bottom:12px;";
        var m = document.createElement("input");
        m.style.cssText = "margin-right:8px;margin-top:8px;margin-bottom:8px;";
        m.setAttribute("value", "url");
        m.setAttribute("type", "radio");
        m.setAttribute("name", "type-embedhtmldialog");
        g = m.cloneNode(!0);
        g.setAttribute("value", "copy");
        f.appendChild(g);
        var k = document.createElement("span");
        mxUtils.write(k, mxResources.get("includeCopyOfMyDiagram"));
        f.appendChild(k);
        mxUtils.br(f);
        f.appendChild(m);
        k = document.createElement("span");
        mxUtils.write(k, mxResources.get("publicDiagramUrl"));
        f.appendChild(k);
        var l = this.getCurrentFile();
        null == c && null != l && l.constructor == window.DriveFile && (k = document.createElement("a"), k.style.paddingLeft = "12px", k.style.color = "gray", k.style.cursor = "pointer", mxUtils.write(k, mxResources.get("share")), f.appendChild(k), mxEvent.addListener(k, "click", mxUtils.bind(this, function() {
            this.hideDialog();
            this.drive.showPermissions(l.getId())
        })));
        g.setAttribute("checked", "checked");
        null == c && m.setAttribute("disabled", "disabled");
        d.appendChild(f);
        var n = this.addLinkSection(d),
            A = this.addCheckbox(d, mxResources.get("zoom"), !0, null, !0);
        mxUtils.write(d, ":");
        var z = document.createElement("input");
        z.setAttribute("type", "text");
        z.style.marginRight = "16px";
        z.style.width = "60px";
        z.style.marginLeft = "4px";
        z.style.marginRight = "12px";
        z.value = "100%";
        d.appendChild(z);
        var B = this.addCheckbox(d, mxResources.get("fit"), !0),
            f = null != this.pages && 1 < this.pages.length,
            C = C = this.addCheckbox(d, mxResources.get("allPages"), f, !f),
            D = this.addCheckbox(d, mxResources.get("layers"), !0),
            E = this.addCheckbox(d, mxResources.get("lightbox"), !0),
            F = this.addEditButton(d, E),
            H = F.getEditInput();
        H.style.marginBottom = "16px";
        mxEvent.addListener(E, "change", function() {
            E.checked ? H.removeAttribute("disabled") : H.setAttribute("disabled", "disabled");
            H.checked && E.checked ? F.getEditSelect().removeAttribute("disabled") : F.getEditSelect().setAttribute("disabled", "disabled")
        });
        a = new CustomDialog(this, d, mxUtils.bind(this, function() {
            e(m.checked ? c : null, A.checked, z.value, n.getTarget(), n.getColor(), B.checked, C.checked, D.checked, E.checked, F.getLink())
        }), null, a, b);
        this.showDialog(a.container, 340, 384, !0, !0);
        g.focus()
    };
    EditorUi.prototype.showPublishLinkDialog = function(a, b, c, e, f, k, l) {
        l = document.createElement("div");
        l.style.whiteSpace = "nowrap";
        var d = document.createElement("h3");
        mxUtils.write(d, a || mxResources.get("link"));
        d.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:12px";
        l.appendChild(d);
        var g = this.getCurrentFile(),
            d = "https://desk.draw.io/support/solutions/articles/16000051941";
        a = 0;
        if (null != g && g.constructor == window.DriveFile && !b) {
            a = 80;
            var d = "https://desk.draw.io/support/solutions/articles/16000039384",
                m = document.createElement("div");
            m.style.cssText = "border-bottom:1px solid lightGray;padding-bottom:14px;padding-top:6px;margin-bottom:14px;text-align:center;";
            var n = document.createElement("div");
            n.style.whiteSpace = "normal";
            mxUtils.write(n, mxResources.get("linkAccountRequired"));
            m.appendChild(n);
            n = mxUtils.button(mxResources.get("share"), mxUtils.bind(this, function() {
                this.drive.showPermissions(g.getId())
            }));
            n.style.marginTop = "12px";
            n.className = "geBtn";
            m.appendChild(n);
            l.appendChild(m);
            n = document.createElement("a");
            n.style.paddingLeft = "12px";
            n.style.color = "gray";
            n.style.fontSize = "14px";
            n.style.cursor = "pointer";
            mxUtils.write(n, mxResources.get("check"));
            m.appendChild(n);
            mxEvent.addListener(n, "click", mxUtils.bind(this, function() {
                this.spinner.spin(document.body, mxResources.get("loading")) && this.getPublicUrl(this.getCurrentFile(), mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    a = new ErrorDialog(this, null, mxResources.get(null != a ? "diagramIsPublic" : "diagramIsNotPublic"), mxResources.get("ok"));
                    this.showDialog(a.container, 300, 80, !0, !1);
                    a.init()
                }))
            }))
        }
        var p = null,
            z = null;
        if (null != c || null != e) a += 30, mxUtils.write(l, mxResources.get("width") + ":"), p = document.createElement("input"), p.setAttribute("type", "text"), p.style.marginRight = "16px", p.style.width = "50px", p.style.marginLeft = "6px", p.style.marginRight = "16px", p.style.marginBottom = "10px", p.value = "100%", l.appendChild(p), mxUtils.write(l, mxResources.get("height") + ":"), z = document.createElement("input"), z.setAttribute("type", "text"), z.style.width = "50px", z.style.marginLeft = "6px", z.style.marginBottom = "10px", z.value = e + "px", l.appendChild(z), mxUtils.br(l);
        var t = this.addLinkSection(l, k);
        c = null != this.pages && 1 < this.pages.length;
        var u = null;
        if (null == g || g.constructor != window.DriveFile || b) u = this.addCheckbox(l, mxResources.get("allPages"), c, !c);
        var D = this.addCheckbox(l, mxResources.get("lightbox"), !0, null, null, !k),
            E = this.addEditButton(l, D),
            F = E.getEditInput();
        k && (F.style.marginLeft = D.style.marginLeft, D.style.display = "none", a -= 30);
        var H = this.addCheckbox(l, mxResources.get("layers"), !0);
        H.style.marginLeft = F.style.marginLeft;
        H.style.marginBottom = "16px";
        H.style.marginTop = "8px";
        mxEvent.addListener(D, "change", function() {
            D.checked ? (H.removeAttribute("disabled"), F.removeAttribute("disabled")) : (H.setAttribute("disabled", "disabled"), F.setAttribute("disabled", "disabled"));
            F.checked && D.checked ? E.getEditSelect().removeAttribute("disabled") : E.getEditSelect().setAttribute("disabled", "disabled")
        });
        b = new CustomDialog(this, l, mxUtils.bind(this, function() {
            f(t.getTarget(), t.getColor(), null == u ? !0 : u.checked, D.checked, E.getLink(), H.checked, null != p ? p.value : null, null != z ? z.value : null)
        }), null, mxResources.get("create"), d);
        this.showDialog(b.container, 340, 254 + a, !0, !0);
        null != p ? (p.focus(), mxClient.IS_GC || mxClient.IS_FF || 5 <= document.documentMode || mxClient.IS_QUIRKS ? p.select() : document.execCommand("selectAll", !1, null)) : t.focus()
    };
    EditorUi.prototype.showRemoteExportDialog = function(a, b, c, e, f) {
        var d = document.createElement("div");
        d.style.whiteSpace = "nowrap";
        var g = document.createElement("h3");
        mxUtils.write(g, mxResources.get("image"));
        g.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:" + (f ? "10" : "4") + "px";
        d.appendChild(g);
        if (f) {
            mxUtils.write(d, mxResources.get("zoom") + ":");
            var m = document.createElement("input");
            m.setAttribute("type", "text");
            m.style.marginRight = "16px";
            m.style.width = "60px";
            m.style.marginLeft = "4px";
            m.style.marginRight = "12px";
            m.value = this.lastExportZoom || "100%";
            d.appendChild(m);
            mxUtils.write(d, mxResources.get("borderWidth") + ":");
            var k = document.createElement("input");
            k.setAttribute("type", "text");
            k.style.marginRight = "16px";
            k.style.width = "60px";
            k.style.marginLeft = "4px";
            k.value = this.lastExportBorder || "0";
            d.appendChild(k);
            mxUtils.br(d)
        }
        var l = this.addCheckbox(d, mxResources.get("selectionOnly"), !1, this.editor.graph.isSelectionEmpty()),
            n = e ? null : this.addCheckbox(d, mxResources.get("includeCopyOfMyDiagram"), !0),
            g = this.editor.graph,
            p = e ? null : this.addCheckbox(d, mxResources.get("transparentBackground"), g.background == mxConstants.NONE || null == g.background);
        null != p && (p.style.marginBottom = "16px");
        a = new CustomDialog(this, d, mxUtils.bind(this, function() {
            var a = parseInt(m.value) / 100 || 1,
                b = parseInt(k.value) || 0;
            c(!l.checked, null != n ? n.checked : !1, null != p ? p.checked : !1, a, b)
        }), null, a, b);
        this.showDialog(a.container, 300, (f ? 25 : 0) + (e ? 125 : 210), !0, !0)
    };
    EditorUi.prototype.showExportDialog = function(a, b, c, e, f, k, l, q) {
        l = null != l ? l : !0;
        var d = document.createElement("div");
        d.style.whiteSpace = "nowrap";
        var g = this.editor.graph,
            m = "jpeg" == q ? 196 : 300,
            n = document.createElement("h3");
        mxUtils.write(n, a);
        n.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:10px";
        d.appendChild(n);
        mxUtils.write(d, mxResources.get("zoom") + ":");
        var p = document.createElement("input");
        p.setAttribute("type", "text");
        p.style.marginRight = "16px";
        p.style.width = "60px";
        p.style.marginLeft = "4px";
        p.style.marginRight = "12px";
        p.value = this.lastExportZoom || "100%";
        d.appendChild(p);
        mxUtils.write(d, mxResources.get("borderWidth") + ":");
        var t = document.createElement("input");
        t.setAttribute("type", "text");
        t.style.marginRight = "16px";
        t.style.width = "60px";
        t.style.marginLeft = "4px";
        t.value = this.lastExportBorder || "0";
        d.appendChild(t);
        mxUtils.br(d);
        var u = this.addCheckbox(d, mxResources.get("transparentBackground"), !1, null, null, "jpeg" != q),
            D = null;
        "dark" == uiTheme && (D = this.addCheckbox(d, mxResources.get("dark"), !0), m += 26);
        var E = this.addCheckbox(d, mxResources.get("selectionOnly"), !1, g.isSelectionEmpty()),
            F = document.createElement("input");
        F.style.marginTop = "16px";
        F.style.marginRight = "8px";
        F.style.marginLeft = "24px";
        F.setAttribute("disabled", "disabled");
        F.setAttribute("type", "checkbox");
        k && (d.appendChild(F), mxUtils.write(d, mxResources.get("crop")), mxUtils.br(d), m += 26, mxEvent.addListener(E, "change", function() {
            E.checked ? F.removeAttribute("disabled") : F.setAttribute("disabled", "disabled")
        }));
        g.isSelectionEmpty() || (F.setAttribute("checked", "checked"), F.defaultChecked = !0);
        var H = this.addCheckbox(d, mxResources.get("shadow"), g.shadowVisible),
            I = document.createElement("input");
        I.style.marginTop = "16px";
        I.style.marginRight = "8px";
        I.setAttribute("type", "checkbox");
        !this.isOffline() && this.canvasSupported || I.setAttribute("disabled", "disabled");
        b && (d.appendChild(I), mxUtils.write(d, mxResources.get("embedImages")), mxUtils.br(d), m += 26);
        var K = null;
        if ("png" == q || "jpeg" == q) K = this.addCheckbox(d, mxResources.get("grid"), !1, this.isOffline() || !this.canvasSupported, !1, !0), m += 26;
        var G = this.addCheckbox(d, mxResources.get("includeCopyOfMyDiagram"), l, null, null, "jpeg" != q),
            L = null != this.pages && 1 < this.pages.length,
            O = this.addCheckbox(d, L ? mxResources.get("allPages") : "", L, !L, null, "jpeg" != q);
        O.style.marginLeft = "24px";
        O.style.marginBottom = "16px";
        L ? m += 26 : O.style.display = "none";
        mxEvent.addListener(G, "change", function() {
            G.checked && L ? O.removeAttribute("disabled") : O.setAttribute("disabled", "disabled")
        });
        l && L || O.setAttribute("disabled", "disabled");
        var J = document.createElement("select");
        J.style.maxWidth = "260px";
        J.style.marginLeft = "8px";
        J.style.marginRight = "10px";
        J.className = "geBtn";
        a = document.createElement("option");
        a.setAttribute("value", "auto");
        mxUtils.write(a, mxResources.get("automatic"));
        J.appendChild(a);
        a = document.createElement("option");
        a.setAttribute("value", "blank");
        mxUtils.write(a, mxResources.get("openInNewWindow"));
        J.appendChild(a);
        a = document.createElement("option");
        a.setAttribute("value", "self");
        mxUtils.write(a, mxResources.get("openInThisWindow"));
        J.appendChild(a);
        "svg" == q && (mxUtils.write(d, mxResources.get("links") + ":"), d.appendChild(J), mxUtils.br(d), mxUtils.br(d), m += 26);
        c = new CustomDialog(this, d, mxUtils.bind(this, function() {
            this.lastExportBorder = t.value;
            this.lastExportZoom = p.value;
            f(p.value, u.checked, !E.checked, H.checked, G.checked, I.checked, t.value, F.checked, !O.checked, J.value, null != K ? K.checked : null, null != D ? D.checked : null)
        }), null, c, e);
        this.showDialog(c.container, 340, m, !0, !0, null, null, null, null, !0);
        p.focus();
        mxClient.IS_GC || mxClient.IS_FF || 5 <= document.documentMode || mxClient.IS_QUIRKS ? p.select() : document.execCommand("selectAll", !1, null)
    };
    EditorUi.prototype.showEmbedImageDialog = function(a, b, c, e, f) {
        var d = document.createElement("div");
        d.style.whiteSpace = "nowrap";
        var g = this.editor.graph;
        if (null != b) {
            var m = document.createElement("h3");
            mxUtils.write(m, b);
            m.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:4px";
            d.appendChild(m)
        }
        var k = this.addCheckbox(d, mxResources.get("fit"), !0),
            l = this.addCheckbox(d, mxResources.get("shadow"), g.shadowVisible && e, !e),
            n = this.addCheckbox(d, c),
            p = this.addCheckbox(d, mxResources.get("lightbox"), !0),
            z = this.addEditButton(d, p),
            B = z.getEditInput(),
            C = 1 < g.model.getChildCount(g.model.getRoot()),
            D = this.addCheckbox(d, mxResources.get("layers"), C, !C);
        D.style.marginLeft = B.style.marginLeft;
        D.style.marginBottom = "12px";
        D.style.marginTop = "8px";
        mxEvent.addListener(p, "change", function() {
            p.checked ? (C && D.removeAttribute("disabled"), B.removeAttribute("disabled")) : (D.setAttribute("disabled", "disabled"), B.setAttribute("disabled", "disabled"));
            B.checked && p.checked ? z.getEditSelect().removeAttribute("disabled") : z.getEditSelect().setAttribute("disabled", "disabled")
        });
        b = new CustomDialog(this, d, mxUtils.bind(this, function() {
            a(k.checked, l.checked, n.checked, p.checked, z.getLink(), D.checked)
        }), null, mxResources.get("embed"), f);
        this.showDialog(b.container, 280, 280, !0, !0)
    };
    EditorUi.prototype.createEmbedImage = function(a, b, c, e, f, k, l, q) {
        function d(b) {
            var d = " ",
                m = "";
            e && (d = " onclick=\"(function(img){if(img.wnd!=null&&!img.wnd.closed){img.wnd.focus();}else{var r=function(evt){if(evt.data=='ready'&&evt.source==img.wnd){img.wnd.postMessage(decodeURIComponent(img.getAttribute('src')),'*');window.removeEventListener('message',r);}};window.addEventListener('message',r);img.wnd=window.open('" + EditorUi.lightboxHost + "/?client=1" + (f ? "&edit=_blank" : "") + (k ? "&layers=1" : "") + "');}})(this);\"", m += "cursor:pointer;");
            a && (m += "max-width:100%;");
            var n = "";
            c && (n = ' width="' + Math.round(g.width) + '" height="' + Math.round(g.height) + '"');
            l('<img src="' + b + '"' + n + ("" != m ? ' style="' + m + '"' : "") + d + "/>")
        }
        var g = this.editor.graph.getGraphBounds();
        if (this.isExportToCanvas()) this.editor.exportToCanvas(mxUtils.bind(this, function(a) {
            var b = e ? this.getFileData(!0) : null;
            a = this.createImageDataUri(a, b, "png");
            d(a)
        }), null, null, null, mxUtils.bind(this, function(a) {
            q({
                message: mxResources.get("unknownError")
            })
        }), null, !0, c ? 2 : 1, null, b, null, null, Editor.defaultBorder);
        else if (b = this.getFileData(!0), g.width * g.height <= MAX_AREA && b.length <= MAX_REQUEST_SIZE) {
            var m = "";
            c && (m = "&w=" + Math.round(2 * g.width) + "&h=" + Math.round(2 * g.height));
            var n = new mxXmlRequest(EXPORT_URL, "format=png&base64=1&embedXml=" + (e ? "1" : "0") + m + "&xml=" + encodeURIComponent(b));
            n.send(mxUtils.bind(this, function() {
                200 <= n.getStatus() && 299 >= n.getStatus() ? d("data:image/png;base64," + n.getText()) : q({
                    message: mxResources.get("unknownError")
                })
            }))
        } else q({
            message: mxResources.get("drawingTooLarge")
        })
    };
    EditorUi.prototype.createEmbedSvg = function(a, b, c, e, f, k, l) {
        var d = this.editor.graph.getSvg(null, null, null, null, null, null, null, null, null, null, !c),
            g = d.getElementsByTagName("a");
        if (null != g)
            for (var m = 0; m < g.length; m++) {
                var n = g[m].getAttribute("href");
                null != n && "#" == n.charAt(0) && "_blank" == g[m].getAttribute("target") && g[m].removeAttribute("target")
            }
        e && d.setAttribute("content", this.getFileData(!0));
        b && this.editor.graph.addSvgShadow(d);
        if (c) {
            var p = " ",
                z = "";
            e && (p = "onclick=\"(function(img){if(img.wnd!=null&&!img.wnd.closed){img.wnd.focus();}else{var r=function(evt){if(evt.data=='ready'&&evt.source==img.wnd){img.wnd.postMessage(decodeURIComponent(img.getAttribute('src')),'*');window.removeEventListener('message',r);}};window.addEventListener('message',r);img.wnd=window.open('" + EditorUi.lightboxHost + "/?client=1" + (f ? "&edit=_blank" : "") + (k ? "&layers=1" : "") + "');}})(this);\"", z += "cursor:pointer;");
            a && (z += "max-width:100%;");
            this.editor.convertImages(d, mxUtils.bind(this, function(a) {
                l('<img src="' + Editor.createSvgDataUri(mxUtils.getXml(a)) + '"' + ("" != z ? ' style="' + z + '"' : "") + p + "/>")
            }))
        } else z = "", e && (d.setAttribute("onclick", "(function(svg){var src=window.event.target||window.event.srcElement;while (src!=null&&src.nodeName.toLowerCase()!='a'){src=src.parentNode;}if(src==null){if(svg.wnd!=null&&!svg.wnd.closed){svg.wnd.focus();}else{var r=function(evt){if(evt.data=='ready'&&evt.source==svg.wnd){svg.wnd.postMessage(decodeURIComponent(svg.getAttribute('content')),'*');window.removeEventListener('message',r);}};window.addEventListener('message',r);svg.wnd=window.open('" + EditorUi.lightboxHost + "/?client=1" + (f ? "&edit=_blank" : "") + (k ? "&layers=1" : "") + "');}}})(this);"), z += "cursor:pointer;"), a && (a = parseInt(d.getAttribute("width")), b = parseInt(d.getAttribute("height")), d.setAttribute("viewBox", "-0.5 -0.5 " + a + " " + b), z += "max-width:100%;max-height:" + b + "px;", d.removeAttribute("height")), "" != z && d.setAttribute("style", z), this.editor.addFontCss(d), this.editor.graph.mathEnabled && this.editor.addMathCss(d), l(mxUtils.getXml(d))
    };
    EditorUi.prototype.timeSince = function(a) {
        a = Math.floor((new Date - a) / 1E3);
        var b = Math.floor(a / 31536E3);
        if (1 < b) return b + " " + mxResources.get("years");
        b = Math.floor(a / 2592E3);
        if (1 < b) return b + " " + mxResources.get("months");
        b = Math.floor(a / 86400);
        if (1 < b) return b + " " + mxResources.get("days");
        b = Math.floor(a / 3600);
        if (1 < b) return b + " " + mxResources.get("hours");
        b = Math.floor(a / 60);
        return 1 < b ? b + " " + mxResources.get("minutes") : 1 == b ? b + " " + mxResources.get("minute") : null
    };
    EditorUi.prototype.decodeNodeIntoGraph = function(a, b) {
        if (null != a) {
            var d = null;
            if ("diagram" == a.nodeName) d = a;
            else if ("mxfile" == a.nodeName) {
                var c = a.getElementsByTagName("diagram");
                if (0 < c.length) {
                    var d = c[0],
                        e = b.getGlobalVariable;
                    b.getGlobalVariable = function(a) {
                        return "page" == a ? d.getAttribute("name") || mxResources.get("pageWithNumber", [1]) : "pagenumber" == a ? 1 : e.apply(this, arguments)
                    }
                }
            }
            null != d && (a = Editor.parseDiagramNode(d))
        }
        c = this.editor.graph;
        try {
            this.editor.graph = b, this.editor.setGraphXml(a)
        } catch (t) {} finally {
            this.editor.graph = c
        }
        return a
    };
    EditorUi.prototype.getPngFileProperties = function(a) {
        var b = 1,
            d = 0;
        if (null != a) {
            if (a.hasAttribute("scale")) {
                var c = parseFloat(a.getAttribute("scale"));
                !isNaN(c) && 0 < c && (b = c)
            }
            a.hasAttribute("border") && (c = parseInt(a.getAttribute("border")), !isNaN(c) && 0 < c && (d = c))
        }
        return {
            scale: b,
            border: d
        }
    };
    EditorUi.prototype.getEmbeddedPng = function(a, b, c, e, f) {
        try {
            var d = this.editor.graph,
                g = null != d.themes && "darkTheme" == d.defaultThemeName,
                m = null;
            if (null != c && 0 < c.length) d = this.createTemporaryGraph(this.editor.graph.getStylesheet()), document.body.appendChild(d.container), this.decodeNodeIntoGraph(this.editor.extractGraphModel(mxUtils.parseXml(c).documentElement, !0), d), m = c;
            else if (g || null != this.pages && this.currentPage != this.pages[0]) {
                var d = this.createTemporaryGraph(d.getStylesheet()),
                    k = d.getGlobalVariable,
                    l = this.pages[0];
                d.getGlobalVariable = function(a) {
                    return "page" == a ? l.getName() : "pagenumber" == a ? 1 : k.apply(this, arguments)
                };
                document.body.appendChild(d.container);
                d.model.setRoot(l.root)
            }
            this.editor.exportToCanvas(mxUtils.bind(this, function(c) {
                try {
                    null == m && (m = this.getFileData(!0, null, null, null, null, null, null, null, null, !1));
                    var e = c.toDataURL("image/png"),
                        e = Editor.writeGraphModelToPng(e, "tEXt", "mxfile", encodeURIComponent(m));
                    a(e.substring(e.lastIndexOf(",") + 1));
                    d != this.editor.graph && d.container.parentNode.removeChild(d.container)
                } catch (z) {
                    null != b && b(z)
                }
            }), null, null, null, mxUtils.bind(this, function(a) {
                null != b && b(a)
            }), null, null, e, null, d.shadowVisible, null, d, f)
        } catch (y) {
            null != b && b(y)
        }
    };
    EditorUi.prototype.getEmbeddedSvg = function(a, b, c, e, f, k, l, q) {
        q = null != q ? q : !0;
        l = b.background;
        l == mxConstants.NONE && (l = null);
        k = b.getSvg(l, null, null, null, null, k);
        b.shadowVisible && b.addSvgShadow(k);
        null != a && k.setAttribute("content", a);
        null != c && k.setAttribute("resource", c);
        if (null != f) this.embedFonts(k, mxUtils.bind(this, function(a) {
            q ? this.editor.convertImages(a, mxUtils.bind(this, function(a) {
                f((e ? "" : '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n') + mxUtils.getXml(a))
            })) : f((e ? "" : '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n') + mxUtils.getXml(a))
        }));
        else return (e ? "" : '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n') + mxUtils.getXml(k)
    };
    EditorUi.prototype.embedFonts = function(a, b) {
        this.editor.loadFonts(mxUtils.bind(this, function() {
            try {
                null != this.editor.resolvedFontCss && this.editor.addFontCss(a, this.editor.resolvedFontCss), this.editor.embedExtFonts(mxUtils.bind(this, function(d) {
                    try {
                        null != d && this.editor.addFontCss(a, d), b(a)
                    } catch (n) {
                        b(a)
                    }
                }))
            } catch (m) {
                b(a)
            }
        }))
    };
    EditorUi.prototype.exportImage = function(a, b, c, e, f, k, l, q, v, x, y, A) {
        v = null != v ? v : "png";
        if (this.spinner.spin(document.body, mxResources.get("exporting"))) {
            var d = this.editor.graph.isSelectionEmpty();
            c = null != c ? c : d;
            null == this.thumbImageCache && (this.thumbImageCache = {});
            try {
                this.editor.exportToCanvas(mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    try {
                        this.saveCanvas(a, f ? this.getFileData(!0, null, null, null, c, q) : null, v, null == this.pages || 0 == this.pages.length, y)
                    } catch (C) {
                        "Invalid image" == C.message ? this.downloadFile(v) : this.handleError(C)
                    }
                }), null, this.thumbImageCache, null, mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    this.handleError(a)
                }), null, c, a || 1, b, e, null, null, k, l, x, A)
            } catch (B) {
                this.spinner.stop(), this.handleError(B)
            }
        }
    };
    EditorUi.prototype.isCorsEnabledForUrl = function(a) {
        return this.editor.isCorsEnabledForUrl(a)
    };
    EditorUi.prototype.importXml = function(a, b, c, e, f) {
        b = null != b ? b : 0;
        c = null != c ? c : 0;
        var d = [];
        try {
            var g = this.editor.graph;
            if (null != a && 0 < a.length) {
                g.model.beginUpdate();
                try {
                    var m = mxUtils.parseXml(a);
                    a = {};
                    var k = this.editor.extractGraphModel(m.documentElement, null != this.pages);
                    if (null != k && "mxfile" == k.nodeName && null != this.pages) {
                        var l = k.getElementsByTagName("diagram");
                        if (1 == l.length) k = Editor.parseDiagramNode(l[0]), null != this.currentPage && (a[l[0].getAttribute("id")] = this.currentPage.getId());
                        else if (1 < l.length) {
                            var m = [],
                                n = 0;
                            null != this.pages && 1 == this.pages.length && this.isDiagramEmpty() && (a[l[0].getAttribute("id")] = this.pages[0].getId(), k = Editor.parseDiagramNode(l[0]), e = !1, n = 1);
                            for (; n < l.length; n++) {
                                var p = l[n].getAttribute("id");
                                l[n].removeAttribute("id");
                                var z = this.updatePageRoot(new DiagramPage(l[n]));
                                a[p] = l[n].getAttribute("id");
                                var B = this.pages.length;
                                null == z.getName() && z.setName(mxResources.get("pageWithNumber", [B + 1]));
                                g.model.execute(new ChangePage(this, z, z, B, !0));
                                m.push(z)
                            }
                            this.updatePageLinks(a, m)
                        }
                    }
                    if (null != k && "mxGraphModel" === k.nodeName && (d = g.importGraphModel(k, b, c, e), null != d))
                        for (n = 0; n < d.length; n++) this.updatePageLinksForCell(a, d[n])
                } finally {
                    g.model.endUpdate()
                }
            }
        } catch (C) {
            if (f) throw C;
            this.handleError(C)
        }
        return d
    };
    EditorUi.prototype.updatePageLinks = function(a, b) {
        for (var d = 0; d < b.length; d++) this.updatePageLinksForCell(a, b[d].root)
    };
    EditorUi.prototype.updatePageLinksForCell = function(a, b) {
        var d = document.createElement("div"),
            c = this.editor.graph,
            e = c.getLinkForCell(b);
        null != e && c.setLinkForCell(b, this.updatePageLink(a, e));
        if (c.isHtmlLabel(b)) {
            d.innerHTML = c.sanitizeHtml(c.getLabel(b));
            for (var g = d.getElementsByTagName("a"), f = !1, k = 0; k < g.length; k++) e = g[k].getAttribute("href"), null != e && (g[k].setAttribute("href", this.updatePageLink(a, e)), f = !0);
            f && c.labelChanged(b, d.innerHTML)
        }
        for (k = 0; k < c.model.getChildCount(b); k++) this.updatePageLinksForCell(a, c.model.getChildAt(b, k))
    };
    EditorUi.prototype.updatePageLink = function(a, b) {
        if ("data:page/id," == b.substring(0, 13)) {
            var d = a[b.substring(b.indexOf(",") + 1)];
            b = null != d ? "data:page/id," + d : null
        } else if ("data:action/json," == b.substring(0, 17)) try {
            var c = JSON.parse(b.substring(17));
            if (null != c.actions) {
                for (var e = 0; e < c.actions.length; e++) {
                    var g = c.actions[e];
                    null != g.open && "data:page/id," == g.open.substring(0, 13) && (d = a[g.open.substring(g.open.indexOf(",") + 1)], null != d ? g.open = "data:page/id," + d : delete g.open)
                }
                b = "data:action/json," + JSON.stringify(c)
            }
        } catch (u) {}
        return b
    };
    EditorUi.prototype.isRemoteVisioFormat = function(a) {
        return /(\.v(sd|dx))($|\?)/i.test(a) || /(\.vs(s|x))($|\?)/i.test(a)
    };
    EditorUi.prototype.importVisio = function(a, b, c, e) {
        e = null != e ? e : a.name;
        c = null != c ? c : mxUtils.bind(this, function(a) {
            this.handleError(a)
        });
        var d = mxUtils.bind(this, function() {
            this.loadingExtensions = !1;
            if (this.doImportVisio) {
                var d = this.isRemoteVisioFormat(e);
                try {
                    var g = "UNKNOWN-VISIO",
                        f = e.lastIndexOf(".");
                    if (0 <= f && f < e.length) g = e.substring(f + 1).toUpperCase();
                    else {
                        var m = e.lastIndexOf("/");
                        0 <= m && m < e.length && (e = e.substring(m + 1))
                    }
                    EditorUi.logEvent({
                        category: g + "-MS-IMPORT-FILE",
                        action: "filename_" + e,
                        label: d ? "remote" : "local"
                    })
                } catch (y) {}
                if (d)
                    if (null == VSD_CONVERT_URL || this.isOffline()) c({
                        message: "conf" == this.getServiceName() ? mxResources.get("vsdNoConfig") : mxResources.get("serviceUnavailableOrBlocked")
                    });
                    else {
                        d = new FormData;
                        d.append("file1", a, e);
                        var k = new XMLHttpRequest;
                        k.open("POST", VSD_CONVERT_URL);
                        k.responseType = "blob";
                        this.addRemoteServiceSecurityCheck(k);
                        k.onreadystatechange = mxUtils.bind(this, function() {
                            if (4 == k.readyState)
                                if (200 <= k.status && 299 >= k.status) try {
                                    var a = k.response;
                                    if ("text/xml" == a.type) {
                                        var d = new FileReader;
                                        d.onload = mxUtils.bind(this, function(a) {
                                            try {
                                                b(a.target.result)
                                            } catch (B) {
                                                c({
                                                    message: mxResources.get("errorLoadingFile")
                                                })
                                            }
                                        });
                                        d.readAsText(a)
                                    } else this.doImportVisio(a, b, c, e)
                                } catch (z) {
                                    c(z)
                                } else c({})
                        });
                        k.send(d)
                    }
                else try {
                    this.doImportVisio(a, b, c, e)
                } catch (y) {
                    c(y)
                }
            } else this.spinner.stop(),
                this.handleError({
                    message: mxResources.get("serviceUnavailableOrBlocked")
                })
        });
        this.doImportVisio || this.loadingExtensions || this.isOffline(!0) ? d() : (this.loadingExtensions = !0, mxscript("js/extensions.min.js", d))
    };
    EditorUi.prototype.importGraphML = function(a, b, c) {
        c = null != c ? c : mxUtils.bind(this, function(a) {
            this.handleError(a)
        });
        var d = mxUtils.bind(this, function() {
            this.loadingExtensions = !1;
            if (this.doImportGraphML) try {
                this.doImportGraphML(a, b, c)
            } catch (p) {
                c(p)
            } else this.spinner.stop(), this.handleError({
                message: mxResources.get("serviceUnavailableOrBlocked")
            })
        });
        this.doImportGraphML || this.loadingExtensions || this.isOffline(!0) ? d() : (this.loadingExtensions = !0, mxscript("js/extensions.min.js", d))
    };
    EditorUi.prototype.exportVisio = function() {
        var a = mxUtils.bind(this, function() {
            this.loadingExtensions = !1;
            if ("undefined" !== typeof VsdxExport) try {
                (new VsdxExport(this)).exportCurrentDiagrams() || this.handleError({
                    message: mxResources.get("unknownError")
                })
            } catch (g) {
                this.handleError(g)
            } else this.spinner.stop(), this.handleError({
                message: mxResources.get("serviceUnavailableOrBlocked")
            })
        });
        "undefined" !== typeof VsdxExport || this.loadingExtensions || this.isOffline(!0) ? a() : (this.loadingExtensions = !0, mxscript("js/extensions.min.js", a))
    };
    EditorUi.prototype.convertLucidChart = function(a, b, c) {
        var d = mxUtils.bind(this, function() {
            this.loadingExtensions = !1;
            if ("undefined" !== typeof window.LucidImporter) {
                try {
                    EditorUi.logEvent({
                        category: "LUCIDCHART-IMPORT-FILE",
                        action: "size_" + a.length
                    }), EditorUi.debug("convertLucidChart", a)
                } catch (p) {}
                try {
                    b(LucidImporter.importState(JSON.parse(a)))
                } catch (p) {
                    null != window.console && console.error(p), c(p)
                }
            } else c({
                message: mxResources.get("serviceUnavailableOrBlocked")
            })
        });
        "undefined" !== typeof window.LucidImporter || this.loadingExtensions || this.isOffline(!0) ? window.setTimeout(d, 0) : (this.loadingExtensions = !0, "1" == urlParams.dev ? mxscript("js/diagramly/Extensions.js", d) : mxscript("js/extensions.min.js", d))
    };
    EditorUi.prototype.generateMermaidImage = function(a, b, c, e) {
        var d = this,
            g = function() {
                try {
                    this.loadingMermaid = !1, b = null != b ? b : EditorUi.defaultMermaidConfig, b.securityLevel = "strict", b.startOnLoad = !1, mermaid.mermaidAPI.initialize(b), mermaid.mermaidAPI.render("geMermaidOutput-" + (new Date).getTime(), a, function(a) {
                        try {
                            if (mxClient.IS_IE || mxClient.IS_IE11) a = a.replace(/ xmlns:\S*="http:\/\/www.w3.org\/XML\/1998\/namespace"/g, "").replace(/ (NS xml|\S*):space="preserve"/g, ' xml:space="preserve"');
                            var b = mxUtils.parseXml(a).getElementsByTagName("svg");
                            if (0 < b.length) {
                                var g = parseFloat(b[0].getAttribute("width")),
                                    f = parseFloat(b[0].getAttribute("height"));
                                c(d.convertDataUri(Editor.createSvgDataUri(a)), g, f)
                            } else e({
                                message: mxResources.get("invalidInput")
                            })
                        } catch (y) {
                            e(y)
                        }
                    })
                } catch (u) {
                    e(u)
                }
            };
        "undefined" !== typeof mermaid || this.loadingMermaid || this.isOffline(!0) ? g() : (this.loadingMermaid = !0, "1" == urlParams.dev ? mxscript("js/mermaid/mermaid.min.js", g) : mxscript("js/extensions.min.js", g))
    };
    EditorUi.prototype.generatePlantUmlImage = function(a, b, c, e) {
        function d(a, b, d) {
            c1 = a >> 2;
            c2 = (a & 3) << 4 | b >> 4;
            c3 = (b & 15) << 2 | d >> 6;
            c4 = d & 63;
            r = "";
            r += g(c1 & 63);
            r += g(c2 & 63);
            r += g(c3 & 63);
            return r += g(c4 & 63)
        }

        function g(a) {
            if (10 > a) return String.fromCharCode(48 + a);
            a -= 10;
            if (26 > a) return String.fromCharCode(65 + a);
            a -= 26;
            if (26 > a) return String.fromCharCode(97 + a);
            a -= 26;
            return 0 == a ? "-" : 1 == a ? "_" : "?"
        }
        var f = new XMLHttpRequest;
        f.open("GET", ("txt" == b ? PLANT_URL + "/txt/" : "png" == b ? PLANT_URL + "/png/" : PLANT_URL + "/svg/") + function(a) {
            r = "";
            for (i = 0; i < a.length; i += 3) r = i + 2 == a.length ? r + d(a.charCodeAt(i), a.charCodeAt(i + 1), 0) : i + 1 == a.length ? r + d(a.charCodeAt(i), 0, 0) : r + d(a.charCodeAt(i), a.charCodeAt(i + 1), a.charCodeAt(i + 2));
            return r
        }(pako.deflateRaw(a, {
            to: "string"
        })), !0);
        "txt" != b && (f.responseType = "blob");
        f.onload = function(a) {
            if (200 <= this.status && 300 > this.status)
                if ("txt" == b) c(this.response);
                else {
                    var d = new FileReader;
                    d.readAsDataURL(this.response);
                    d.onloadend = function(a) {
                        var b = new Image;
                        b.onload = function() {
                            try {
                                var a = b.width,
                                    g = b.height;
                                if (0 == a && 0 == g) {
                                    var f = d.result,
                                        m = f.indexOf(","),
                                        k = decodeURIComponent(escape(atob(f.substring(m + 1)))),
                                        l = mxUtils.parseXml(k).getElementsByTagName("svg");
                                    0 < l.length && (a = parseFloat(l[0].getAttribute("width")), g = parseFloat(l[0].getAttribute("height")))
                                }
                                c(d.result, a, g)
                            } catch (F) {
                                e(F)
                            }
                        };
                        b.src = d.result
                    };
                    d.onerror = function(a) {
                        e(a)
                    }
                }
            else e(a)
        };
        f.onerror = function(a) {
            e(a)
        };
        f.send()
    };
    EditorUi.prototype.insertAsPreText = function(a, b, c) {
        var d = this.editor.graph,
            e = null;
        d.getModel().beginUpdate();
        try {
            e = d.insertVertex(null, null, "<pre>" + a + "</pre>", b, c, 1, 1, "text;html=1;align=left;verticalAlign=top;"), d.updateCellSize(e, !0)
        } finally {
            d.getModel().endUpdate()
        }
        return e
    };
    EditorUi.prototype.insertTextAt = function(a, b, c, e, f, k, l) {
        k = null != k ? k : !0;
        l = null != l ? l : !0;
        if (null != a)
            if (Graph.fileSupport && !this.isOffline() && (new XMLHttpRequest).upload && this.isRemoteFileFormat(a)) this.parseFile(new Blob([a.replace(/\s+/g, " ")], {
                type: "application/octet-stream"
            }), mxUtils.bind(this, function(a) {
                4 == a.readyState && 200 <= a.status && 299 >= a.status && this.editor.graph.setSelectionCells(this.insertTextAt(a.responseText, b, c, !0))
            }));
            else if ("data:" == a.substring(0, 5) || !this.isOffline() && (f || /\.(gif|jpg|jpeg|tiff|png|svg)$/i.test(a))) {
            var d = this.editor.graph;
            if ("data:application/pdf;base64," == a.substring(0, 28)) {
                var g = Editor.extractGraphModelFromPdf(a);
                if (null != g && 0 < g.length) return this.importXml(g, b, c, k, !0)
            }
            if ("data:image/png;base64," == a.substring(0, 22) && (g = this.extractGraphModelFromPng(a), null != g && 0 < g.length)) return this.importXml(g, b, c, k, !0);
            if ("data:image/svg+xml;" == a.substring(0, 19)) try {
                g = null;
                "data:image/svg+xml;base64," == a.substring(0, 26) ? (g = a.substring(a.indexOf(",") + 1), g = window.atob && !mxClient.IS_SF ? atob(g) : Base64.decode(g, !0)) : g = decodeURIComponent(a.substring(a.indexOf(",") + 1));
                var m = this.importXml(g, b, c, k, !0);
                if (0 < m.length) return m
            } catch (A) {}
            this.loadImage(a, mxUtils.bind(this, function(e) {
                if ("data:" == a.substring(0, 5)) this.resizeImage(e, a, mxUtils.bind(this, function(a, e, g) {
                    d.setSelectionCell(d.insertVertex(null, null, "", d.snap(b), d.snap(c), e, g, "shape=image;verticalLabelPosition=bottom;labelBackgroundColor=#ffffff;verticalAlign=top;aspect=fixed;imageAspect=0;image=" + this.convertDataUri(a) + ";"))
                }), l, this.maxImageSize);
                else {
                    var g = Math.min(1, Math.min(this.maxImageSize / e.width, this.maxImageSize / e.height)),
                        f = Math.round(e.width * g);
                    e = Math.round(e.height * g);
                    d.setSelectionCell(d.insertVertex(null, null, "", d.snap(b), d.snap(c), f, e, "shape=image;verticalLabelPosition=bottom;labelBackgroundColor=#ffffff;verticalAlign=top;aspect=fixed;imageAspect=0;image=" + a + ";"))
                }
            }), mxUtils.bind(this, function() {
                var g = null;
                d.getModel().beginUpdate();
                try {
                    g = d.insertVertex(d.getDefaultParent(), null, a, d.snap(b), d.snap(c), 1, 1, "text;" + (e ? "html=1;" : "")), d.updateCellSize(g), d.fireEvent(new mxEventObject("textInserted", "cells", [g]))
                } finally {
                    d.getModel().endUpdate()
                }
                d.setSelectionCell(g)
            }))
        } else {
            a = Graph.zapGremlins(mxUtils.trim(a));
            if (this.isCompatibleString(a)) return this.importXml(a, b, c, k);
            if (0 < a.length)
                if (this.isLucidChartData(a)) this.convertLucidChart(a, mxUtils.bind(this, function(a) {
                    this.editor.graph.setSelectionCells(this.importXml(a, b, c, k))
                }), mxUtils.bind(this, function(a) {
                    this.handleError(a)
                }));
                else {
                    d = this.editor.graph;
                    f = null;
                    d.getModel().beginUpdate();
                    try {
                        f = d.insertVertex(d.getDefaultParent(), null, "", d.snap(b), d.snap(c), 1, 1, "text;whiteSpace=wrap;" + (e ? "html=1;" : ""));
                        d.fireEvent(new mxEventObject("textInserted", "cells", [f]));
                        "<" == a.charAt(0) && a.indexOf(">") == a.length - 1 && (a = mxUtils.htmlEntities(a));
                        a.length > this.maxTextBytes && (a = a.substring(0, this.maxTextBytes) + "...");
                        f.value = a;
                        d.updateCellSize(f);
                        if (0 < this.maxTextWidth && f.geometry.width > this.maxTextWidth) {
                            var n = d.getPreferredSizeForCell(f, this.maxTextWidth);
                            f.geometry.width = n.width;
                            f.geometry.height = n.height
                        }
                        Graph.isLink(f.value) && d.setLinkForCell(f, f.value);
                        f.geometry.width += d.gridSize;
                        f.geometry.height += d.gridSize
                    } finally {
                        d.getModel().endUpdate()
                    }
                    return [f]
                }
        }
        return []
    };
    EditorUi.prototype.formatFileSize = function(a) {
        var b = -1;
        do a /= 1024, b++; while (1024 < a);
        return Math.max(a, .1).toFixed(1) + " kB; MB; GB; TB;PB;EB;ZB;YB".split(";")[b]
    };
    EditorUi.prototype.convertDataUri = function(a) {
        if ("data:" == a.substring(0, 5)) {
            var b = a.indexOf(";");
            0 < b && (a = a.substring(0, b) + a.substring(a.indexOf(",", b + 1)))
        }
        return a
    };
    EditorUi.prototype.isRemoteFileFormat = function(a, b) {
        return /(\"contentType\":\s*\"application\/gliffy\+json\")/.test(a)
    };
    EditorUi.prototype.isLucidChartData = function(a) {
        return null != a && ('{"state":"{\\"Properties\\":' == a.substring(0, 26) || '{"Properties":' == a.substring(0, 14))
    };
    EditorUi.prototype.importLocalFile = function(a, b) {
        if (a && Graph.fileSupport) {
            if (null == this.importFileInputElt) {
                var d = document.createElement("input");
                d.setAttribute("type", "file");
                mxEvent.addListener(d, "change", mxUtils.bind(this, function() {
                    null != d.files && (this.importFiles(d.files, null, null, this.maxImageSize), d.type = "", d.type = "file", d.value = "")
                }));
                d.style.display = "none";
                document.body.appendChild(d);
                this.importFileInputElt = d
            }
            this.importFileInputElt.click()
        } else {
            window.openNew = !1;
            window.openKey = "import";
            if (!b) {
                var c = Editor.useLocalStorage;
                Editor.useLocalStorage = !a
            }
            window.openFile = new OpenFile(mxUtils.bind(this, function(a) {
                this.hideDialog(a)
            }));
            window.openFile.setConsumer(mxUtils.bind(this, function(a, b) {
                if (null != b && Graph.fileSupport && /(\.v(dx|sdx?))($|\?)/i.test(b)) {
                    var d = new Blob([a], {
                        type: "application/octet-stream"
                    });
                    this.importVisio(d, mxUtils.bind(this, function(a) {
                        this.importXml(a, 0, 0, !0)
                    }), null, b)
                } else this.editor.graph.setSelectionCells(this.importXml(a, 0, 0, !0))
            }));
            this.showDialog((new OpenDialog(this)).container, 360, 220, !0, !0, function() {
                window.openFile = null
            });
            if (!b) {
                var e = this.dialog,
                    g = e.close;
                this.dialog.close = mxUtils.bind(this, function(a) {
                    Editor.useLocalStorage = c;
                    g.apply(e, arguments);
                    a && null == this.getCurrentFile() && "1" != urlParams.embed && this.showSplash()
                })
            }
        }
    };
    EditorUi.prototype.importZipFile = function(a, b, c) {
        var d = this,
            e = mxUtils.bind(this, function() {
                this.loadingExtensions = !1;
                "undefined" !== typeof JSZip ? JSZip.loadAsync(a).then(function(e) {
                    if (0 == Object.keys(e.files).length) c();
                    else {
                        var g = 0,
                            f, k = !1;
                        e.forEach(function(a, d) {
                            var e = d.name.toLowerCase();
                            "diagram/diagram.xml" == e ? (k = !0, d.async("string").then(function(a) {
                                0 == a.indexOf("<mxfile ") ? b(a) : c()
                            })) : 0 == e.indexOf("versions/") && (e = parseInt(e.substr(9)), e > g && (g = e, f = d))
                        });
                        0 < g ? f.async("string").then(function(e) {
                            !d.isOffline() && (new XMLHttpRequest).upload && d.isRemoteFileFormat(e, a.name) ? d.parseFile(new Blob([e], {
                                type: "application/octet-stream"
                            }), mxUtils.bind(this, function(a) {
                                4 == a.readyState && (200 <= a.status && 299 >= a.status ? b(a.responseText) : c())
                            }), a.name) : c()
                        }) : k || c()
                    }
                }, function(a) {
                    c(a)
                }) : c()
            });
        "undefined" !== typeof JSZip || this.loadingExtensions || this.isOffline(!0) ? e() : (this.loadingExtensions = !0, mxscript("js/extensions.min.js", e))
    };
    EditorUi.prototype.importFile = function(a, b, c, e, f, k, l, q, v, x, y) {
        x = null != x ? x : !0;
        var d = !1,
            g = null,
            m = mxUtils.bind(this, function(a) {
                var b = null;
                null != a && "<mxlibrary" == a.substring(0, 10) ? this.loadLibrary(new LocalLibrary(this, a, l)) : b = this.importXml(a, c, e, x);
                null != q && q(b)
            });
        "image" == b.substring(0, 5) ? (v = !1, "image/png" == b.substring(0, 9) && (b = y ? null : this.extractGraphModelFromPng(a), null != b && 0 < b.length && (g = this.importXml(b, c, e, x), v = !0)), v || (b = this.editor.graph, y = a.indexOf(";"), 0 < y && (a = a.substring(0, y) + a.substring(a.indexOf(",", y + 1))), x && b.isGridEnabled() && (c = b.snap(c), e = b.snap(e)), g = [b.insertVertex(null, null, "", c, e, f, k, "shape=image;verticalLabelPosition=bottom;labelBackgroundColor=#ffffff;verticalAlign=top;aspect=fixed;imageAspect=0;image=" + a + ";")])) : /(\.*<graphml )/.test(a) ? (d = !0, this.importGraphML(a, m)) : null != v && null != l && (/(\.v(dx|sdx?))($|\?)/i.test(l) || /(\.vs(x|sx?))($|\?)/i.test(l)) ? (d = !0, this.importVisio(v, m)) : !this.isOffline() && (new XMLHttpRequest).upload && this.isRemoteFileFormat(a, l) ? (d = !0, this.parseFile(null != v ? v : new Blob([a], {
            type: "application/octet-stream"
        }), mxUtils.bind(this, function(a) {
            4 == a.readyState && (200 <= a.status && 299 >= a.status ? m(a.responseText) : null != q && q(null))
        }), l)) : 0 == a.indexOf("PK") && null != v ? (d = !0, this.importZipFile(v, m, mxUtils.bind(this, function() {
            g = this.insertTextAt(this.validateFileData(a), c, e, !0, null, x);
            q(g)
        }))) : /(\.v(sd|dx))($|\?)/i.test(l) || /(\.vs(s|x))($|\?)/i.test(l) || (g = this.insertTextAt(this.validateFileData(a), c, e, !0, null, x));
        d || null == q || q(g);
        return g
    };
    EditorUi.prototype.importFiles = function(a, b, c, e, f, k, l, q, v, x, y, A) {
        e = null != e ? e : this.maxImageSize;
        x = null != x ? x : this.maxImageBytes;
        var d = null != b && null != c,
            g = !0;
        b = null != b ? b : 0;
        c = null != c ? c : 0;
        var m = !1;
        if (!mxClient.IS_CHROMEAPP && null != a)
            for (var n = y || this.resampleThreshold, p = 0; p < a.length; p++)
                if ("image/" == a[p].type.substring(0, 6) && a[p].size > n) {
                    m = !0;
                    break
                }
        var t = mxUtils.bind(this, function() {
            var m = this.editor.graph,
                n = m.gridSize;
            f = null != f ? f : mxUtils.bind(this, function(a, b, c, e, g, f, k, m, l) {
                try {
                    return null != a && "<mxlibrary" == a.substring(0, 10) ? (this.spinner.stop(), this.loadLibrary(new LocalLibrary(this, a, k)), null) : this.importFile(a, b, c, e, g, f, k, m, l, d, A)
                } catch (T) {
                    return this.handleError(T), null
                }
            });
            k = null != k ? k : mxUtils.bind(this, function(a) {
                m.setSelectionCells(a)
            });
            if (this.spinner.spin(document.body, mxResources.get("loading")))
                for (var p = a.length, z = p, t = [], B = mxUtils.bind(this, function(a, b) {
                        t[a] = b;
                        if (0 == --z) {
                            this.spinner.stop();
                            if (null != q) q(t);
                            else {
                                var d = [];
                                m.getModel().beginUpdate();
                                try {
                                    for (var c = 0; c < t.length; c++) {
                                        var e = t[c]();
                                        null != e && (d = d.concat(e))
                                    }
                                } finally {
                                    m.getModel().endUpdate()
                                }
                            }
                            k(d)
                        }
                    }), u = 0; u < p; u++) mxUtils.bind(this, function(d) {
                    var k = a[d];
                    if (null != k) {
                        var p = new FileReader;
                        p.onload = mxUtils.bind(this, function(a) {
                            if (null == l || l(k))
                                if ("image/" == k.type.substring(0, 6))
                                    if ("image/svg" == k.type.substring(0, 9)) {
                                        var p = a.target.result,
                                            z = p.indexOf(","),
                                            t = decodeURIComponent(escape(atob(p.substring(z + 1)))),
                                            u = mxUtils.parseXml(t),
                                            t = u.getElementsByTagName("svg");
                                        if (0 < t.length) {
                                            var t = t[0],
                                                q = A ? null : t.getAttribute("content");
                                            null != q && "<" != q.charAt(0) && "%" != q.charAt(0) && (q = unescape(window.atob ? atob(q) : Base64.decode(q, !0)));
                                            null != q && "%" == q.charAt(0) && (q = decodeURIComponent(q));
                                            null == q || "<mxfile " !== q.substring(0, 8) && "<mxGraphModel " !== q.substring(0, 14) ? B(d, mxUtils.bind(this, function() {
                                                try {
                                                    if (p.substring(0, z + 1), null != u) {
                                                        var a = u.getElementsByTagName("svg");
                                                        if (0 < a.length) {
                                                            var g = a[0],
                                                                l = g.getAttribute("width"),
                                                                t = g.getAttribute("height"),
                                                                l = null != l && "%" != l.charAt(l.length - 1) ? parseFloat(l) : NaN,
                                                                t = null != t && "%" != t.charAt(t.length - 1) ? parseFloat(t) : NaN,
                                                                B = g.getAttribute("viewBox");
                                                            if (null == B || 0 == B.length) g.setAttribute("viewBox", "0 0 " + l + " " + t);
                                                            else if (isNaN(l) || isNaN(t)) {
                                                                var q = B.split(" ");
                                                                3 < q.length && (l = parseFloat(q[2]), t = parseFloat(q[3]))
                                                            }
                                                            p = Editor.createSvgDataUri(mxUtils.getXml(g));
                                                            var v = Math.min(1, Math.min(e / Math.max(1, l)), e / Math.max(1, t)),
                                                                C = f(p, k.type, b + d * n, c + d * n, Math.max(1, Math.round(l * v)), Math.max(1, Math.round(t * v)), k.name);
                                                            if (isNaN(l) || isNaN(t)) {
                                                                var x = new Image;
                                                                x.onload = mxUtils.bind(this, function() {
                                                                    l = Math.max(1, x.width);
                                                                    t = Math.max(1, x.height);
                                                                    C[0].geometry.width = l;
                                                                    C[0].geometry.height = t;
                                                                    g.setAttribute("viewBox", "0 0 " + l + " " + t);
                                                                    p = Editor.createSvgDataUri(mxUtils.getXml(g));
                                                                    var a = p.indexOf(";");
                                                                    0 < a && (p = p.substring(0, a) + p.substring(p.indexOf(",", a + 1)));
                                                                    m.setCellStyles("image", p, [C[0]])
                                                                });
                                                                x.src = Editor.createSvgDataUri(mxUtils.getXml(g))
                                                            }
                                                            return C
                                                        }
                                                    }
                                                } catch (fa) {}
                                                return null
                                            })) : B(d, mxUtils.bind(this, function() {
                                                return f(q, "text/xml", b + d * n, c + d * n, 0, 0, k.name)
                                            }))
                                        } else B(d, mxUtils.bind(this, function() {
                                            return null
                                        }))
                                    } else {
                                        t = !1;
                                        if ("image/png" == k.type) {
                                            var v = A ? null : this.extractGraphModelFromPng(a.target.result);
                                            if (null != v && 0 < v.length) {
                                                var C = new Image;
                                                C.src = a.target.result;
                                                B(d, mxUtils.bind(this, function() {
                                                    return f(v, "text/xml", b + d * n, c + d * n, C.width, C.height, k.name)
                                                }));
                                                t = !0
                                            }
                                        }
                                        t || (mxClient.IS_CHROMEAPP ? (this.spinner.stop(), this.showError(mxResources.get("error"), mxResources.get("dragAndDropNotSupported"), mxResources.get("cancel"), mxUtils.bind(this, function() {}), null, mxResources.get("ok"), mxUtils.bind(this, function() {
                                            this.actions.get("import").funct()
                                        }))) : this.loadImage(a.target.result, mxUtils.bind(this, function(m) {
                                            this.resizeImage(m, a.target.result, mxUtils.bind(this, function(m, l, p) {
                                                B(d, mxUtils.bind(this, function() {
                                                    if (null != m && m.length < x) {
                                                        var z = g && this.isResampleImage(a.target.result, y) ? Math.min(1, Math.min(e / l, e / p)) : 1;
                                                        return f(m, k.type, b + d * n, c + d * n, Math.round(l * z), Math.round(p * z), k.name)
                                                    }
                                                    this.handleError({
                                                        message: mxResources.get("imageTooBig")
                                                    });
                                                    return null
                                                }))
                                            }), g, e, y)
                                        }), mxUtils.bind(this, function() {
                                            this.handleError({
                                                message: mxResources.get("invalidOrMissingFile")
                                            })
                                        })))
                                    }
                            else p = a.target.result, f(p, k.type, b + d * n, c + d * n, 240, 160, k.name, function(a) {
                                B(d, function() {
                                    return a
                                })
                            }, k)
                        });
                        /(\.v(dx|sdx?))($|\?)/i.test(k.name) || /(\.vs(x|sx?))($|\?)/i.test(k.name) ? f(null, k.type, b + d * n, c + d * n, 240, 160, k.name, function(a) {
                            B(d, function() {
                                return a
                            })
                        }, k) : "image" == k.type.substring(0, 5) || "application/pdf" == k.type ? p.readAsDataURL(k) : p.readAsText(k)
                    }
                })(u)
        });
        if (m) {
            m = [];
            for (p = 0; p < a.length; p++) m.push(a[p]);
            a = m;
            this.confirmImageResize(function(a) {
                g = a;
                t()
            }, v)
        } else t()
    };
    EditorUi.prototype.confirmImageResize = function(a, b) {
        b = null != b ? b : !1;
        var d = null != this.spinner && null != this.spinner.pause ? this.spinner.pause() : function() {},
            c = isLocalStorage || mxClient.IS_CHROMEAPP ? mxSettings.getResizeImages() : null,
            e = function(c, e) {
                if (c || b) mxSettings.setResizeImages(c ? e : null), mxSettings.save();
                d();
                a(e)
            };
        null == c || b ? this.showDialog((new ConfirmDialog(this, mxResources.get("resizeLargeImages"), function(a) {
            e(a, !0)
        }, function(a) {
            e(a, !1)
        }, mxResources.get("resize"), mxResources.get("actualSize"), '<img style="margin-top:8px;" src="' + Editor.loResImage + '"/>', '<img style="margin-top:8px;" src="' + Editor.hiResImage + '"/>', isLocalStorage || mxClient.IS_CHROMEAPP)).container, 340, isLocalStorage || mxClient.IS_CHROMEAPP ? 220 : 200, !0, !0) : e(!1, c)
    };
    EditorUi.prototype.parseFile = function(a, b, c) {
        c = null != c ? c : a.name;
        var d = new FormData;
        d.append("format", "xml");
        d.append("upfile", a, c);
        var e = new XMLHttpRequest;
        e.open("POST", OPEN_URL);
        e.onreadystatechange = function() {
            b(e)
        };
        e.send(d);
        try {
            EditorUi.logEvent({
                category: "GLIFFY-IMPORT-FILE",
                action: "size_" + a.size
            })
        } catch (t) {}
    };
    EditorUi.prototype.isResampleImage = function(a, b) {
        b = null != b ? b : this.resampleThreshold;
        return a.length > b
    };
    EditorUi.prototype.resizeImage = function(a, b, c, e, f, k) {
        f = null != f ? f : this.maxImageSize;
        var d = Math.max(1, a.width),
            g = Math.max(1, a.height);
        if (e && this.isResampleImage(b, k)) try {
            var m = Math.max(d / f, g / f);
            if (1 < m) {
                var l = Math.round(d / m),
                    n = Math.round(g / m),
                    p = document.createElement("canvas");
                p.width = l;
                p.height = n;
                p.getContext("2d").drawImage(a, 0, 0, l, n);
                var z = p.toDataURL();
                if (z.length < b.length) {
                    var t = document.createElement("canvas");
                    t.width = l;
                    t.height = n;
                    var C = t.toDataURL();
                    z !== C && (b = z, d = l, g = n)
                }
            }
        } catch (D) {}
        c(b, d, g)
    };
    EditorUi.prototype.extractGraphModelFromPng = function(a) {
        return Editor.extractGraphModelFromPng(a)
    };
    EditorUi.prototype.loadImage = function(a, b, c) {
        try {
            var d = new Image;
            d.onload = function() {
                d.width = 0 < d.width ? d.width : 120;
                d.height = 0 < d.height ? d.height : 120;
                b(d)
            };
            null != c && (d.onerror = c);
            d.src = a
        } catch (p) {
            if (null != c) c(p);
            else throw p;
        }
    };
    var b = EditorUi.prototype.init;
    EditorUi.prototype.init = function() {
        mxStencilRegistry.allowEval = mxStencilRegistry.allowEval && !this.isOfflineApp();
        "undefined" !== typeof window.mxSettings && (this.formatWidth = mxSettings.getFormatWidth());
        var a = this,
            c = this.editor.graph;
        "dark" == uiTheme && (c.view.defaultGridColor = mxGraphView.prototype.defaultDarkGridColor);
        c.cellEditor.editPlantUmlData = function(b, d, e) {
            var f = JSON.parse(e);
            d = new TextareaDialog(a, mxResources.get("plantUml") + ":", f.data, function(d) {
                null != d && a.spinner.spin(document.body, mxResources.get("inserting")) && a.generatePlantUmlImage(d, f.format, function(e, g, k) {
                    a.spinner.stop();
                    c.getModel().beginUpdate();
                    try {
                        if ("txt" == f.format) c.labelChanged(b, "<pre>" + e + "</pre>"), c.updateCellSize(b, !0);
                        else {
                            c.setCellStyles("image", a.convertDataUri(e), [b]);
                            var m = c.model.getGeometry(b);
                            null != m && (m = m.clone(), m.width = g, m.height = k, c.cellsResized([b], [m], !1))
                        }
                        c.setAttributeForCell(b, "plantUmlData", JSON.stringify({
                            data: d,
                            format: f.format
                        }))
                    } finally {
                        c.getModel().endUpdate()
                    }
                }, function(b) {
                    a.handleError(b)
                })
            }, null, null, 400, 220);
            a.showDialog(d.container, 420, 300, !0, !0);
            d.init()
        };
        c.cellEditor.editMermaidData = function(b, d, e) {
            var f = JSON.parse(e);
            d = new TextareaDialog(a, mxResources.get("mermaid") + ":", f.data, function(d) {
                null != d && a.spinner.spin(document.body, mxResources.get("inserting")) && a.generateMermaidImage(d, f.config, function(e, g, k) {
                    a.spinner.stop();
                    c.getModel().beginUpdate();
                    try {
                        c.setCellStyles("image", e, [b]);
                        var m = c.model.getGeometry(b);
                        null != m && (m = m.clone(), m.width = Math.max(m.width, g), m.height = Math.max(m.height, k), c.cellsResized([b], [m], !1));
                        c.setAttributeForCell(b, "mermaidData", JSON.stringify({
                            data: d,
                            config: f.config
                        }, null, 2))
                    } finally {
                        c.getModel().endUpdate()
                    }
                }, function(b) {
                    a.handleError(b)
                })
            }, null, null, 400, 220);
            a.showDialog(d.container, 420, 300, !0, !0);
            d.init()
        };
        var e = c.cellEditor.startEditing;
        c.cellEditor.startEditing = function(b, d) {
            try {
                var f = this.graph.getAttributeForCell(b, "plantUmlData");
                if (null != f) this.editPlantUmlData(b, d, f);
                else if (f = this.graph.getAttributeForCell(b, "mermaidData"), null != f) this.editMermaidData(b, d, f);
                else {
                    var g = c.getCellStyle(b);
                    "1" == mxUtils.getValue(g, "metaEdit", "0") ? a.showDataDialog(b) : e.apply(this, arguments)
                }
            } catch (E) {
                a.handleError(E)
            }
        };
        c.getLinkTitle = function(b) {
            return a.getLinkTitle(b)
        };
        c.customLinkClicked = function(b) {
            var d = !1;
            try {
                a.handleCustomLink(b), d = !0
            } catch (C) {
                a.handleError(C)
            }
            return d
        };
        var f = this.clearDefaultStyle;
        this.clearDefaultStyle = function() {
            f.apply(this, arguments)
        };
        this.isOffline() || "undefined" === typeof window.EditDataDialog || (EditDataDialog.placeholderHelpLink = "https://desk.draw.io/support/solutions/articles/16000051979");
        if (/viewer\.diagrams\.net$/.test(window.location.hostname) || /embed\.diagrams\.net$/.test(window.location.hostname)) this.editor.editBlankUrl = "https://app.diagrams.net/";
        var k = a.editor.getEditBlankUrl;
        this.editor.getEditBlankUrl = function(b) {
            b = null != b ? b : "";
            if (null != a.pages && null != a.currentPage)
                for (var d = 0; d < a.pages.length; d++)
                    if (a.pages[d] == a.currentPage) {
                        0 < d && (b += (0 < b.length ? "&" : "?") + "page=" + d);
                        break
                    }
                    "1" == urlParams.dev && (b += (0 < b.length ? "&" : "?") + "dev=1&drawdev=1");
            return k.apply(this, arguments)
        };
        var l = c.addClickHandler;
        c.addClickHandler = function(a, b, d) {
            var e = b;
            b = function(a, b) {
                if (null == b) {
                    var d = mxEvent.getSource(a);
                    "a" == d.nodeName.toLowerCase() && (b = d.getAttribute("href"))
                }
                null != b && c.isCustomLink(b) && (mxEvent.isTouchEvent(a) || !mxEvent.isPopupTrigger(a)) && c.customLinkClicked(b) && mxEvent.consume(a);
                null != e && e(a, b)
            };
            l.call(this, a, b, d)
        };
        b.apply(this, arguments);
        mxClient.IS_SVG && this.editor.graph.addSvgShadow(c.view.canvas.ownerSVGElement, null, !0);
        a.actions.get("print").funct = function() {
            a.showDialog((new PrintDialog(a)).container, 360, null != a.pages && 1 < a.pages.length ? 450 : 370, !0, !0)
        };
        this.defaultFilename = mxResources.get("untitledDiagram");
        var u = c.getExportVariables;
        c.getExportVariables = function() {
            var b = u.apply(this, arguments),
                d = a.getCurrentFile();
            null != d && (b.filename = d.getTitle());
            b.pagecount = null != a.pages ? a.pages.length : 1;
            b.page = null != a.currentPage ? a.currentPage.getName() : "";
            b.pagenumber = null != a.pages && null != a.currentPage ? mxUtils.indexOf(a.pages, a.currentPage) + 1 : 1;
            return b
        };
        var q = c.getGlobalVariable;
        c.getGlobalVariable = function(b) {
            var d = a.getCurrentFile();
            return "filename" == b && null != d ? d.getTitle() : "page" == b && null != a.currentPage ? a.currentPage.getName() : "pagenumber" == b ? null != a.currentPage && null != a.pages ? mxUtils.indexOf(a.pages, a.currentPage) + 1 : 1 : "pagecount" == b ? null != a.pages ? a.pages.length : 1 : q.apply(this, arguments)
        };
        var v = c.labelLinkClicked;
        c.labelLinkClicked = function(a, b, d) {
            var e = b.getAttribute("href");
            if (null == e || !c.isCustomLink(e) || !mxEvent.isTouchEvent(d) && mxEvent.isPopupTrigger(d)) v.apply(this, arguments);
            else {
                if (!c.isEnabled() || null != a && c.isCellLocked(a.cell)) c.customLinkClicked(e), c.getRubberband().reset();
                mxEvent.consume(d)
            }
        };
        this.editor.getOrCreateFilename = function() {
            var b = a.defaultFilename,
                d = a.getCurrentFile();
            null != d && (b = null != d.getTitle() ? d.getTitle() : b);
            return b
        };
        var x = this.actions.get("print");
        x.setEnabled(!mxClient.IS_IOS || !navigator.standalone);
        x.visible = x.isEnabled();
        if (!this.editor.chromeless || this.editor.editable) this.keyHandler.bindAction(70, !0, "find"), this.keyHandler.bindAction(67, !0, "copyStyle", !0), this.keyHandler.bindAction(86, !0, "pasteStyle", !0), this.keyHandler.bindAction(77, !0, "editGeometry", !0), this.keyHandler.bindAction(88, !0, "insertText", !0), this.keyHandler.bindAction(75, !0, "insertRectangle"), this.keyHandler.bindAction(75, !0, "insertEllipse", !0), mxClient.IS_CHROMEAPP || EditorUi.isElectronApp || (this.altShiftActions[83] = "synchronize"), this.installImagePasteHandler(), this.installNativeClipboardHandler();
        this.spinner = this.createSpinner(document.body.clientWidth / 2 - 2, Math.max(document.body.clientHeight || 0, document.documentElement.clientHeight || 0) / 2, 24);
        Graph.fileSupport && this.editor.graph.addListener(mxEvent.EDITING_STARTED, mxUtils.bind(this, function(a) {
            var b = this.editor.graph,
                d = b.cellEditor.text2,
                c = null;
            null != d && (mxEvent.addListener(d, "dragleave", function(a) {
                null != c && (c.parentNode.removeChild(c), c = null);
                a.stopPropagation();
                a.preventDefault()
            }), mxEvent.addListener(d, "dragover", mxUtils.bind(this, function(a) {
                null == c && (!mxClient.IS_IE || 10 < document.documentMode) && (c = this.highlightElement(d));
                a.stopPropagation();
                a.preventDefault()
            })), mxEvent.addListener(d, "drop", mxUtils.bind(this, function(a) {
                null != c && (c.parentNode.removeChild(c), c = null);
                if (0 < a.dataTransfer.files.length) this.importFiles(a.dataTransfer.files, 0, 0, this.maxImageSize, function(a, d, c, e, f, g) {
                    b.insertImage(a, f, g)
                }, function() {}, function(a) {
                    return "image/" == a.type.substring(0, 6)
                }, function(a) {
                    for (var b = 0; b < a.length; b++) a[b]()
                }, mxEvent.isControlDown(a));
                else if (0 <= mxUtils.indexOf(a.dataTransfer.types, "text/uri-list")) {
                    var d = a.dataTransfer.getData("text/uri-list");
                    /\.(gif|jpg|jpeg|tiff|png|svg)$/i.test(d) ? this.loadImage(decodeURIComponent(d), mxUtils.bind(this, function(a) {
                        var c = Math.max(1, a.width);
                        a = Math.max(1, a.height);
                        var e = this.maxImageSize,
                            e = Math.min(1, Math.min(e / Math.max(1, c)), e / Math.max(1, a));
                        b.insertImage(decodeURIComponent(d), c * e, a * e)
                    })) : document.execCommand("insertHTML", !1, a.dataTransfer.getData("text/plain"))
                } else 0 <= mxUtils.indexOf(a.dataTransfer.types, "text/html") ? document.execCommand("insertHTML", !1, a.dataTransfer.getData("text/html")) : 0 <= mxUtils.indexOf(a.dataTransfer.types, "text/plain") && document.execCommand("insertHTML", !1, a.dataTransfer.getData("text/plain"));
                a.stopPropagation();
                a.preventDefault()
            })))
        }));
        "undefined" !== typeof window.mxSettings && (x = this.editor.graph.view, x.setUnit(mxSettings.getUnit()), x.addListener("unitChanged", function(a, b) {
            mxSettings.setUnit(b.getProperty("unit"));
            mxSettings.save()
        }), this.ruler = !this.canvasSupported || 9 == document.documentMode || "1" != urlParams.ruler && !mxSettings.isRulerOn() || this.editor.isChromelessView() && !this.editor.editable ? null : new mxDualRuler(this, x.unit), this.refresh());
        if ("1" == urlParams.styledev) {
            x = document.getElementById("geFooter");
            null != x && (this.styleInput = document.createElement("input"), this.styleInput.setAttribute("type", "text"), this.styleInput.style.position = "absolute", this.styleInput.style.top = "14px", this.styleInput.style.left = "2px", this.styleInput.style.width = "98%", this.styleInput.style.visibility = "hidden", this.styleInput.style.opacity = "0.9", mxEvent.addListener(this.styleInput, "change", mxUtils.bind(this, function() {
                this.editor.graph.getModel().setStyle(this.editor.graph.getSelectionCell(), this.styleInput.value)
            })), x.appendChild(this.styleInput), this.editor.graph.getSelectionModel().addListener(mxEvent.CHANGE, mxUtils.bind(this, function(a, b) {
                if (0 < this.editor.graph.getSelectionCount()) {
                    var d = this.editor.graph.getSelectionCell(),
                        d = this.editor.graph.getModel().getStyle(d);
                    this.styleInput.value = d || "";
                    this.styleInput.style.visibility = "visible"
                } else this.styleInput.style.visibility = "hidden"
            })));
            var y = this.isSelectionAllowed;
            this.isSelectionAllowed = function(a) {
                return mxEvent.getSource(a) == this.styleInput ? !0 : y.apply(this, arguments)
            }
        }
        x = document.getElementById("geInfo");
        null != x && x.parentNode.removeChild(x);
        if (Graph.fileSupport && (!this.editor.chromeless || this.editor.editable)) {
            var A = null;
            mxEvent.addListener(c.container, "dragleave", function(a) {
                c.isEnabled() && (null != A && (A.parentNode.removeChild(A), A = null), a.stopPropagation(), a.preventDefault())
            });
            mxEvent.addListener(c.container, "dragover", mxUtils.bind(this, function(a) {
                null == A && (!mxClient.IS_IE || 10 < document.documentMode) && (A = this.highlightElement(c.container));
                null != this.sidebar && this.sidebar.hideTooltip();
                a.stopPropagation();
                a.preventDefault()
            }));
            mxEvent.addListener(c.container, "drop", mxUtils.bind(this, function(a) {
                null != A && (A.parentNode.removeChild(A), A = null);
                if (c.isEnabled()) {
                    var b = mxUtils.convertPoint(c.container, mxEvent.getClientX(a), mxEvent.getClientY(a)),
                        d = c.view.translate,
                        e = c.view.scale,
                        f = b.x / e - d.x,
                        g = b.y / e - d.y;
                    if (0 < a.dataTransfer.files.length) mxEvent.isAltDown(a) && (g = f = null), this.importFiles(a.dataTransfer.files, f, g, this.maxImageSize, null, null, null, null, mxEvent.isControlDown(a), null, null, mxEvent.isShiftDown(a));
                    else {
                        mxEvent.isAltDown(a) && (g = f = 0);
                        var k = 0 <= mxUtils.indexOf(a.dataTransfer.types, "text/uri-list") ? a.dataTransfer.getData("text/uri-list") : null,
                            b = this.extractGraphModelFromEvent(a, null != this.pages);
                        if (null != b) c.setSelectionCells(this.importXml(b, f, g, !0));
                        else if (0 <= mxUtils.indexOf(a.dataTransfer.types, "text/html")) {
                            var m = a.dataTransfer.getData("text/html"),
                                b = document.createElement("div");
                            b.innerHTML = c.sanitizeHtml(m);
                            var l = null,
                                d = b.getElementsByTagName("img");
                            null != d && 1 == d.length ? (m = d[0].getAttribute("src"), null == m && (m = d[0].getAttribute("srcset")), /\.(gif|jpg|jpeg|tiff|png|svg)$/i.test(m) || (l = !0)) : (d = b.getElementsByTagName("a"), null != d && 1 == d.length ? m = d[0].getAttribute("href") : (b = b.getElementsByTagName("pre"), null != b && 1 == b.length && (m = mxUtils.getTextContent(b[0]))));
                            var n = !0,
                                p = mxUtils.bind(this, function() {
                                    c.setSelectionCells(this.insertTextAt(m, f, g, !0, l, null, n))
                                });
                            l && null != m && m.length > this.resampleThreshold ? this.confirmImageResize(function(a) {
                                n = a;
                                p()
                            }, mxEvent.isControlDown(a)) : p()
                        } else null != k && /\.(gif|jpg|jpeg|tiff|png|svg)$/i.test(k) ? this.loadImage(decodeURIComponent(k), mxUtils.bind(this, function(a) {
                            var b = Math.max(1, a.width);
                            a = Math.max(1, a.height);
                            var d = this.maxImageSize,
                                d = Math.min(1, Math.min(d / Math.max(1, b)), d / Math.max(1, a));
                            c.setSelectionCell(c.insertVertex(null, null, "", f, g, b * d, a * d, "shape=image;verticalLabelPosition=bottom;labelBackgroundColor=#ffffff;verticalAlign=top;aspect=fixed;imageAspect=0;image=" + k + ";"))
                        }), mxUtils.bind(this, function(a) {
                            c.setSelectionCells(this.insertTextAt(k, f, g, !0))
                        })) : 0 <= mxUtils.indexOf(a.dataTransfer.types, "text/plain") && c.setSelectionCells(this.insertTextAt(a.dataTransfer.getData("text/plain"), f, g, !0))
                    }
                }
                a.stopPropagation();
                a.preventDefault()
            }), !1)
        }
        this.initPages();
        "1" == urlParams.embed && this.initializeEmbedMode();
        this.installSettings()
    };
    EditorUi.prototype.installImagePasteHandler = function() {
        if (!mxClient.IS_IE) {
            var a = this.editor.graph;
            a.container.addEventListener("paste", mxUtils.bind(this, function(b) {
                if (!mxEvent.isConsumed(b)) try {
                    for (var d = b.clipboardData || b.originalEvent.clipboardData, c = !1, e = 0; e < d.types.length; e++)
                        if ("text/" === d.types[e].substring(0, 5)) {
                            c = !0;
                            break
                        }
                    if (!c) {
                        var f = d.items;
                        for (index in f) {
                            var g = f[index];
                            if ("file" === g.kind) {
                                if (a.isEditing()) this.importFiles([g.getAsFile()], 0, 0, this.maxImageSize, function(b, d, c, e, f, g) {
                                    a.insertImage(b, f, g)
                                }, function() {}, function(a) {
                                    return "image/" == a.type.substring(0, 6)
                                }, function(a) {
                                    for (var b = 0; b < a.length; b++) a[b]()
                                });
                                else {
                                    var k = this.editor.graph.getInsertPoint();
                                    this.importFiles([g.getAsFile()], k.x, k.y, this.maxImageSize);
                                    mxEvent.consume(b)
                                }
                                break
                            }
                        }
                    }
                } catch (v) {}
            }), !1)
        }
    };
    EditorUi.prototype.installNativeClipboardHandler = function() {
        function a() {
            window.setTimeout(function() {
                c.innerHTML = "&nbsp;";
                c.focus();
                document.execCommand("selectAll", !1, null)
            }, 0)
        }
        var b = this.editor.graph,
            c = document.createElement("div");
        c.setAttribute("autocomplete", "off");
        c.setAttribute("autocorrect", "off");
        c.setAttribute("autocapitalize", "off");
        c.setAttribute("spellcheck", "false");
        c.style.textRendering = "optimizeSpeed";
        c.style.fontFamily = "monospace";
        c.style.wordBreak = "break-all";
        c.style.background = "transparent";
        c.style.color = "transparent";
        c.style.position = "absolute";
        c.style.whiteSpace = "nowrap";
        c.style.overflow = "hidden";
        c.style.display = "block";
        c.style.fontSize = "1";
        c.style.zIndex = "-1";
        c.style.resize = "none";
        c.style.outline = "none";
        c.style.width = "1px";
        c.style.height = "1px";
        mxUtils.setOpacity(c, 0);
        c.contentEditable = !0;
        c.innerHTML = "&nbsp;";
        var e = !1;
        this.keyHandler.bindControlKey(88, null);
        this.keyHandler.bindControlKey(67, null);
        this.keyHandler.bindControlKey(86, null);
        mxEvent.addListener(document, "keydown", mxUtils.bind(this, function(a) {
            var d = mxEvent.getSource(a);
            null == b.container || !b.isEnabled() || b.isMouseDown || b.isEditing() || null != this.dialog || "INPUT" == d.nodeName || "TEXTAREA" == d.nodeName || 224 != a.keyCode && (mxClient.IS_MAC || 17 != a.keyCode) && (!mxClient.IS_MAC || 91 != a.keyCode && 93 != a.keyCode) || e || (c.style.left = b.container.scrollLeft + 10 + "px", c.style.top = b.container.scrollTop + 10 + "px", b.container.appendChild(c), e = !0, mxClient.IS_QUIRKS ? window.setTimeout(function() {
                c.focus();
                document.execCommand("selectAll", !1, null)
            }, 0) : (c.focus(), document.execCommand("selectAll", !1, null)))
        }));
        mxEvent.addListener(document, "keyup", mxUtils.bind(this, function(a) {
            var d = a.keyCode;
            window.setTimeout(mxUtils.bind(this, function() {
                !e || 224 != d && 17 != d && 91 != d || (e = !1, b.isEditing() || null != this.dialog || null == b.container || b.container.focus(), c.parentNode.removeChild(c), null == this.dialog && mxUtils.clearSelection())
            }), 0)
        }));
        mxEvent.addListener(c, "copy", mxUtils.bind(this, function(d) {
            if (b.isEnabled()) try {
                mxClipboard.copy(b), this.copyCells(c), a()
            } catch (u) {
                this.handleError(u)
            }
        }));
        mxEvent.addListener(c, "cut", mxUtils.bind(this, function(d) {
            if (b.isEnabled()) try {
                mxClipboard.copy(b), this.copyCells(c, !0), a()
            } catch (u) {
                this.handleError(u)
            }
        }));
        mxEvent.addListener(c, "paste", mxUtils.bind(this, function(a) {
            b.isEnabled() && !b.isCellLocked(b.getDefaultParent()) && ((new Date).getTime(), c.innerHTML = "&nbsp;", c.focus(), null != a.clipboardData && this.pasteCells(a, c, !0, !0), mxEvent.isConsumed(a) || window.setTimeout(mxUtils.bind(this, function() {
                this.pasteCells(a, c, !1, !0)
            }), 0))
        }), !0);
        var f = this.isSelectionAllowed;
        this.isSelectionAllowed = function(a) {
            return mxEvent.getSource(a) == c ? !0 : f.apply(this, arguments)
        }
    };
    EditorUi.prototype.getLinkTitle = function(a) {
        var b = Graph.prototype.getLinkTitle.apply(this, arguments);
        if ("data:page/id," == a.substring(0, 13)) {
            var d = a.indexOf(",");
            0 < d && (b = this.getPageById(a.substring(d + 1)), b = null != b ? b.getName() : mxResources.get("pageNotFound"))
        } else "data:" == a.substring(0, 5) && (b = mxResources.get("action"));
        return b
    };
    EditorUi.prototype.handleCustomLink = function(a) {
        if ("data:page/id," == a.substring(0, 13)) {
            var b = a.indexOf(",");
            if (a = this.getPageById(a.substring(b + 1))) this.selectPage(a);
            else throw Error(mxResources.get("pageNotFound") || "Page not found");
        } else this.editor.graph.handleCustomLink(a)
    };
    EditorUi.prototype.isSettingsEnabled = function() {
        return "undefined" !== typeof window.mxSettings && (isLocalStorage || mxClient.IS_CHROMEAPP)
    };
    EditorUi.prototype.installSettings = function() {
        if (this.isSettingsEnabled()) {
            ColorDialog.recentColors = mxSettings.getRecentColors();
            if (isLocalStorage) try {
                window.addEventListener("storage", mxUtils.bind(this, function(a) {
                    a.key == mxSettings.key && (mxSettings.load(), ColorDialog.recentColors = mxSettings.getRecentColors(), this.menus.customFonts = mxSettings.getCustomFonts())
                }), !1)
            } catch (d) {}
            this.fireEvent(new mxEventObject("styleChanged", "keys", [], "values", [], "cells", []));
            this.menus.customFonts = mxSettings.getCustomFonts();
            this.addListener("customFontsChanged", mxUtils.bind(this, function(a, b) {
                var d = b.getProperty("customFonts");
                this.menus.customFonts = d;
                mxSettings.setCustomFonts(d);
                mxSettings.save()
            }));
            this.editor.graph.connectionHandler.setCreateTarget(mxSettings.isCreateTarget());
            this.fireEvent(new mxEventObject("copyConnectChanged"));
            this.addListener("copyConnectChanged", mxUtils.bind(this, function(a, b) {
                mxSettings.setCreateTarget(this.editor.graph.connectionHandler.isCreateTarget());
                mxSettings.save()
            }));
            this.editor.graph.pageFormat = mxSettings.getPageFormat();
            this.addListener("pageFormatChanged", mxUtils.bind(this, function(a, b) {
                mxSettings.setPageFormat(this.editor.graph.pageFormat);
                mxSettings.save()
            }));
            this.editor.graph.view.gridColor = mxSettings.getGridColor("dark" == uiTheme);
            this.addListener("gridColorChanged", mxUtils.bind(this, function(a, b) {
                mxSettings.setGridColor(this.editor.graph.view.gridColor, "dark" == uiTheme);
                mxSettings.save()
            }));
            if (mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) this.editor.addListener("autosaveChanged", mxUtils.bind(this, function(a, b) {
                mxSettings.setAutosave(this.editor.autosave);
                mxSettings.save()
            })), this.editor.autosave = mxSettings.getAutosave();
            null != this.sidebar && this.sidebar.showPalette("search", mxSettings.settings.search);
            this.editor.chromeless && !this.editor.editable || null == this.sidebar || !(mxSettings.settings.isNew || 8 >= parseInt(mxSettings.settings.version || 0)) || (this.toggleScratchpad(), mxSettings.save());
            this.addListener("formatWidthChanged", function() {
                mxSettings.setFormatWidth(this.formatWidth);
                mxSettings.save()
            })
        }
    };
    EditorUi.prototype.copyCells = function(a, b) {
        var d = this.editor.graph;
        if (d.isSelectionEmpty()) a.innerHTML = "";
        else {
            var c = mxUtils.sortCells(d.model.getTopmostCells(d.getSelectionCells())),
                e = mxUtils.getXml(d.encodeCells(c));
            mxUtils.setTextContent(a, encodeURIComponent(e));
            b ? (d.removeCells(c, !1), d.lastPasteXml = null) : (d.lastPasteXml = e, d.pasteCounter = 0);
            a.focus();
            document.execCommand("selectAll", !1, null)
        }
    };
    EditorUi.prototype.pasteCells = function(a, b, c, e) {
        if (!mxEvent.isConsumed(a)) {
            var d = b,
                f = !1;
            if (c && null != a.clipboardData && a.clipboardData.getData) {
                var g = a.clipboardData.getData("text/html");
                if (null != g && 0 < g.length) {
                    var k = "<meta " == g.substring(0, 6),
                        d = document.createElement("div");
                    d.innerHTML = (k ? '<meta charset="utf-8">' : "") + this.editor.graph.sanitizeHtml(g);
                    f = !0;
                    g = d.getElementsByTagName("style");
                    if (null != g)
                        for (; 0 < g.length;) g[0].parentNode.removeChild(g[0]);
                    null != d.firstChild && d.firstChild.nodeType == mxConstants.NODETYPE_ELEMENT && null != d.firstChild.nextSibling && d.firstChild.nextSibling.nodeType == mxConstants.NODETYPE_ELEMENT && "META" == d.firstChild.nodeName && "A" == d.firstChild.nextSibling.nodeName && null == d.firstChild.nextSibling.nextSibling && (g = null == d.firstChild.nextSibling.innerText ? mxUtils.getTextContent(d.firstChild.nextSibling) : d.firstChild.nextSibling.innerText, g == d.firstChild.nextSibling.getAttribute("href") && (mxUtils.setTextContent(d, g), f = !1));
                    k = k && null != d.firstChild ? d.firstChild.nextSibling : d.firstChild;
                    null != k && null == k.nextSibling && k.nodeType == mxConstants.NODETYPE_ELEMENT && "IMG" == k.nodeName && (g = k.getAttribute("src"), null != g && (mxUtils.setTextContent(d, g), f = !1));
                    f && Graph.removePasteFormatting(d)
                } else g = a.clipboardData.getData("text/plain"), null != g && 0 < g.length && (d = document.createElement("div"), mxUtils.setTextContent(d, g))
            }
            k = d.getElementsByTagName("span");
            if (null != k && 0 < k.length && "application/vnd.lucid.chart.objects" === k[0].getAttribute("data-lucid-type")) c = k[0].getAttribute("data-lucid-content"), null != c && 0 < c.length && (this.convertLucidChart(c, mxUtils.bind(this, function(a) {
                var b = this.editor.graph;
                b.lastPasteXml == a ? b.pasteCounter++ : (b.lastPasteXml = a, b.pasteCounter = 0);
                var d = b.pasteCounter * b.gridSize;
                b.setSelectionCells(this.importXml(a, d, d));
                b.scrollCellToVisible(b.getSelectionCell())
            }), mxUtils.bind(this, function(a) {
                this.handleError(a)
            })), mxEvent.consume(a));
            else {
                f = f ? d.innerHTML : mxUtils.trim(null == d.innerText ? mxUtils.getTextContent(d) : d.innerText);
                g = !1;
                try {
                    var m = f.lastIndexOf("%3E");
                    0 <= m && m < f.length - 3 && (f = f.substring(0, m + 3))
                } catch (C) {}
                try {
                    var k = d.getElementsByTagName("span"),
                        l = null != k && 0 < k.length ? mxUtils.trim(decodeURIComponent(k[0].textContent)) : decodeURIComponent(f);
                    this.isCompatibleString(l) && (g = !0, f = l)
                } catch (C) {}
                try {
                    var n = this.editor.graph;
                    if (null != f && 0 < f.length) {
                        n.lastPasteXml == f ? n.pasteCounter++ : (n.lastPasteXml = f, n.pasteCounter = 0);
                        var A = n.pasteCounter * n.gridSize;
                        if (g || this.isCompatibleString(f)) n.setSelectionCells(this.importXml(f, A, A));
                        else if (e && 1 == n.getSelectionCount()) {
                            var z = n.getStartEditingCell(n.getSelectionCell(), a);
                            /\.(gif|jpg|jpeg|tiff|png|svg)$/i.test(f) && "image" == n.getCurrentCellStyle(z)[mxConstants.STYLE_SHAPE] ? n.setCellStyles(mxConstants.STYLE_IMAGE, f, [z]) : (n.labelChanged(z, f), Graph.isLink(f) && n.setLinkForCell(z, f));
                            n.setSelectionCell(z)
                        } else {
                            var B = n.getInsertPoint();
                            n.isMouseInsertPoint() && (A = 0, n.lastPasteXml == f && 0 < n.pasteCounter && n.pasteCounter--);
                            n.setSelectionCells(this.insertTextAt(f, B.x + A, B.y + A, !0))
                        }
                        n.isSelectionEmpty() || (n.scrollCellToVisible(n.getSelectionCell()), null != this.hoverIcons && this.hoverIcons.update(n.view.getState(n.getSelectionCell())));
                        try {
                            mxEvent.consume(a)
                        } catch (C) {}
                    } else c || (n.lastPasteXml = null, n.pasteCounter = 0)
                } catch (C) {
                    this.handleError(C)
                }
            }
        }
        b.innerHTML = "&nbsp;"
    };
    EditorUi.prototype.addFileDropHandler = function(a) {
        if (Graph.fileSupport)
            for (var b = null, d = 0; d < a.length; d++) mxEvent.addListener(a[d], "dragleave", function(a) {
                    null != b && (b.parentNode.removeChild(b), b = null);
                    a.stopPropagation();
                    a.preventDefault()
                }), mxEvent.addListener(a[d], "dragover", mxUtils.bind(this, function(a) {
                    (this.editor.graph.isEnabled() || "1" != urlParams.embed) && null == b && (!mxClient.IS_IE || 10 < document.documentMode && 12 > document.documentMode) && (b = this.highlightElement());
                    a.stopPropagation();
                    a.preventDefault()
                })),
                mxEvent.addListener(a[d], "drop", mxUtils.bind(this, function(a) {
                    null != b && (b.parentNode.removeChild(b), b = null);
                    if (this.editor.graph.isEnabled() || "1" != urlParams.embed)
                        if (0 < a.dataTransfer.files.length) this.hideDialog(), "1" == urlParams.embed ? this.importFiles(a.dataTransfer.files, 0, 0, this.maxImageSize, null, null, null, null, !mxEvent.isControlDown(a) && !mxEvent.isShiftDown(a)) : this.openFiles(a.dataTransfer.files, !0);
                        else {
                            var d = this.extractGraphModelFromEvent(a);
                            if (null == d) {
                                var c = null != a.dataTransfer ? a.dataTransfer : a.clipboardData;
                                null != c && (10 == document.documentMode || 11 == document.documentMode ? d = c.getData("Text") : (d = null, d = 0 <= mxUtils.indexOf(c.types, "text/uri-list") ? a.dataTransfer.getData("text/uri-list") : 0 <= mxUtils.indexOf(c.types, "text/html") ? c.getData("text/html") : null, null != d && 0 < d.length ? (c = document.createElement("div"), c.innerHTML = this.editor.graph.sanitizeHtml(d), c = c.getElementsByTagName("img"), 0 < c.length && (d = c[0].getAttribute("src"))) : 0 <= mxUtils.indexOf(c.types, "text/plain") && (d = c.getData("text/plain"))), null != d && ("data:image/png;base64," == d.substring(0, 22) ? (d = this.extractGraphModelFromPng(d), null != d && 0 < d.length && this.openLocalFile(d, null, !0)) : !this.isOffline() && this.isRemoteFileFormat(d) ? (new mxXmlRequest(OPEN_URL, "format=xml&data=" + encodeURIComponent(d))).send(mxUtils.bind(this, function(a) {
                                    200 <= a.getStatus() && 299 >= a.getStatus() && this.openLocalFile(a.getText(), null, !0)
                                })) : /^https?:\/\//.test(d) && (null == this.getCurrentFile() ? window.location.hash = "#U" + encodeURIComponent(d) : window.openWindow((mxClient.IS_CHROMEAPP ? EditorUi.drawHost + "/" : "https://" + location.host + "/") + window.location.search + "#U" + encodeURIComponent(d)))))
                            } else this.openLocalFile(d, null, !0)
                        }
                    a.stopPropagation();
                    a.preventDefault()
                }))
    };
    EditorUi.prototype.highlightElement = function(a) {
        var b = 0,
            d = 0,
            c, e;
        if (null == a) {
            e = document.body;
            var f = document.documentElement;
            c = (e.clientWidth || f.clientWidth) - 3;
            e = Math.max(e.clientHeight || 0, f.clientHeight) - 3
        } else b = a.offsetTop, d = a.offsetLeft, c = a.clientWidth, e = a.clientHeight;
        f = document.createElement("div");
        f.style.zIndex = mxPopupMenu.prototype.zIndex + 2;
        f.style.border = "3px dotted rgb(254, 137, 12)";
        f.style.pointerEvents = "none";
        f.style.position = "absolute";
        f.style.top = b + "px";
        f.style.left = d + "px";
        f.style.width = Math.max(0, c - 3) + "px";
        f.style.height = Math.max(0, e - 3) + "px";
        null != a && a.parentNode == this.editor.graph.container ? this.editor.graph.container.appendChild(f) : document.body.appendChild(f);
        return f
    };
    EditorUi.prototype.stringToCells = function(a) {
        a = mxUtils.parseXml(a);
        var b = this.editor.extractGraphModel(a.documentElement);
        a = [];
        if (null != b) {
            var d = new mxCodec(b.ownerDocument),
                c = new mxGraphModel;
            d.decode(b, c);
            b = c.getChildAt(c.getRoot(), 0);
            for (d = 0; d < c.getChildCount(b); d++) a.push(c.getChildAt(b, d))
        }
        return a
    };
    EditorUi.prototype.openFileHandle = function(a, b, c, e, f) {
        if (null != b && 0 < b.length) {
            !this.useCanvasForExport && /(\.png)$/i.test(b) ? b = b.substring(0, b.length - 4) + ".drawio" : /(\.pdf)$/i.test(b) && (b = b.substring(0, b.length - 4) + ".drawio");
            var d = mxUtils.bind(this, function(a) {
                b = 0 <= b.lastIndexOf(".") ? b.substring(0, b.lastIndexOf(".")) + ".drawio" : b + ".drawio";
                if ("<mxlibrary" == a.substring(0, 10)) {
                    null == this.getCurrentFile() && "1" != urlParams.embed && this.openLocalFile(this.emptyDiagramXml, this.defaultFilename, e);
                    try {
                        this.loadLibrary(new LocalLibrary(this, a, b))
                    } catch (v) {
                        this.handleError(v, mxResources.get("errorLoadingFile"))
                    }
                } else this.openLocalFile(a, b, e)
            });
            if (/(\.v(dx|sdx?))($|\?)/i.test(b) || /(\.vs(x|sx?))($|\?)/i.test(b)) this.importVisio(c, mxUtils.bind(this, function(a) {
                this.spinner.stop();
                d(a)
            }));
            else if (/(\.*<graphml )/.test(a)) this.importGraphML(a, mxUtils.bind(this, function(a) {
                this.spinner.stop();
                d(a)
            }));
            else if (Graph.fileSupport && !this.isOffline() && (new XMLHttpRequest).upload && this.isRemoteFileFormat(a, b)) this.parseFile(c, mxUtils.bind(this, function(a) {
                4 == a.readyState && (this.spinner.stop(), 200 <= a.status && 299 >= a.status ? d(a.responseText) : this.handleError({
                    message: mxResources.get(413 == a.status ? "drawingTooLarge" : "invalidOrMissingFile")
                }, mxResources.get("errorLoadingFile")))
            }));
            else if (this.isLucidChartData(a)) /(\.json)$/i.test(b) && (b = b.substring(0, b.length - 5) + ".drawio"), this.convertLucidChart(a, mxUtils.bind(this, function(a) {
                this.spinner.stop();
                this.openLocalFile(a, b, e)
            }), mxUtils.bind(this, function(a) {
                this.spinner.stop();
                this.handleError(a)
            }));
            else if ("<mxlibrary" == a.substring(0, 10)) {
                this.spinner.stop();
                null == this.getCurrentFile() && "1" != urlParams.embed && this.openLocalFile(this.emptyDiagramXml, this.defaultFilename, e);
                try {
                    this.loadLibrary(new LocalLibrary(this, a, c.name))
                } catch (q) {
                    this.handleError(q, mxResources.get("errorLoadingFile"))
                }
            } else if (0 == a.indexOf("PK")) this.importZipFile(c, mxUtils.bind(this, function(a) {
                this.spinner.stop();
                d(a)
            }), mxUtils.bind(this, function() {
                this.spinner.stop();
                this.openLocalFile(a, b, e)
            }));
            else {
                if ("image/png" == c.type.substring(0, 9)) a = this.extractGraphModelFromPng(a);
                else if ("application/pdf" == c.type) {
                    var g = Editor.extractGraphModelFromPdf(a);
                    null != g && (a = g)
                }
                this.spinner.stop();
                this.openLocalFile(a, b, e, f, null != f ? c : null)
            }
        }
    };
    EditorUi.prototype.openFiles = function(a, b) {
        if (this.spinner.spin(document.body, mxResources.get("loading")))
            for (var d = 0; d < a.length; d++) mxUtils.bind(this, function(a) {
                var d = new FileReader;
                d.onload = mxUtils.bind(this, function(d) {
                    try {
                        this.openFileHandle(d.target.result, a.name, a, b)
                    } catch (u) {
                        this.handleError(u)
                    }
                });
                d.onerror = mxUtils.bind(this, function(a) {
                    this.spinner.stop();
                    this.handleError(a);
                    window.openFile = null
                });
                "image" !== a.type.substring(0, 5) && "application/pdf" !== a.type || "image/svg" === a.type.substring(0, 9) ? d.readAsText(a) : d.readAsDataURL(a)
            })(a[d])
    };
    EditorUi.prototype.openLocalFile = function(a, b, c, e, f) {
        var d = this.getCurrentFile(),
            g = mxUtils.bind(this, function() {
                window.openFile = null;
                if (null == b && null != this.getCurrentFile() && this.isDiagramEmpty()) {
                    var d = mxUtils.parseXml(a);
                    null != d && (this.editor.setGraphXml(d.documentElement), this.editor.graph.selectAll())
                } else this.fileLoaded(new LocalFile(this, a, b || this.defaultFilename, c, e, f))
            });
        if (null != a && 0 < a.length) null == d || !d.isModified() && (mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) ? g() : (mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) && null != d && d.isModified() ? this.confirm(mxResources.get("allChangesLost"), null, g, mxResources.get("cancel"), mxResources.get("discardChanges")) : (window.openFile = new OpenFile(function() {
            window.openFile = null
        }), window.openFile.setData(a, b), window.openWindow(this.getUrl(), null, mxUtils.bind(this, function() {
            null != d && d.isModified() ? this.confirm(mxResources.get("allChangesLost"), null, g, mxResources.get("cancel"), mxResources.get("discardChanges")) : g()
        })));
        else throw Error(mxResources.get("notADiagramFile"));
    };
    EditorUi.prototype.getBasenames = function() {
        var a = {};
        if (null != this.pages)
            for (var b = 0; b < this.pages.length; b++) this.updatePageRoot(this.pages[b]), this.addBasenamesForCell(this.pages[b].root, a);
        else this.addBasenamesForCell(this.editor.graph.model.getRoot(), a);
        var b = [],
            c;
        for (c in a) b.push(c);
        return b
    };
    EditorUi.prototype.addBasenamesForCell = function(a, b) {
        function d(a) {
            if (null != a) {
                var d = a.lastIndexOf(".");
                0 < d && (a = a.substring(d + 1, a.length));
                null == b[a] && (b[a] = !0)
            }
        }
        var c = this.editor.graph,
            e = c.getCellStyle(a);
        d(mxStencilRegistry.getBasenameForStencil(e[mxConstants.STYLE_SHAPE]));
        c.model.isEdge(a) && (d(mxMarker.getPackageForType(e[mxConstants.STYLE_STARTARROW])), d(mxMarker.getPackageForType(e[mxConstants.STYLE_ENDARROW])));
        for (var e = c.model.getChildCount(a), f = 0; f < e; f++) this.addBasenamesForCell(c.model.getChildAt(a, f), b)
    };
    EditorUi.prototype.setGraphEnabled = function(a) {
        this.diagramContainer.style.visibility = a ? "" : "hidden";
        this.formatContainer.style.visibility = a ? "" : "hidden";
        this.sidebarFooterContainer.style.display = a ? "" : "none";
        this.sidebarContainer.style.display = a ? "" : "none";
        this.hsplit.style.display = a ? "" : "none";
        this.editor.graph.setEnabled(a);
        null != this.ruler && (this.ruler.hRuler.container.style.visibility = a ? "" : "hidden", this.ruler.vRuler.container.style.visibility = a ? "" : "hidden");
        null != this.tabContainer && (this.tabContainer.style.visibility = a ? "" : "hidden");
        a || (null != this.actions.outlineWindow && this.actions.outlineWindow.window.setVisible(!1), null != this.actions.layersWindow && this.actions.layersWindow.window.setVisible(!1), null != this.menus.tagsWindow && this.menus.tagsWindow.window.setVisible(!1), null != this.menus.findWindow && this.menus.findWindow.window.setVisible(!1))
    };
    EditorUi.prototype.initializeEmbedMode = function() {
        this.setGraphEnabled(!1);
        (window.opener || window.parent) != window && ("1" != urlParams.spin || this.spinner.spin(document.body, mxResources.get("loading"))) && this.installMessageHandler(mxUtils.bind(this, function(a, b, c) {
            this.spinner.stop();
            this.addEmbedButtons();
            this.setGraphEnabled(!0);
            if (null == a || 0 == a.length) a = this.emptyDiagramXml;
            this.setCurrentFile(new LocalFile(this, a, this.defaultFilename, !0));
            this.setFileData(a);
            this.editor.isChromelessView() ? this.editor.graph.isLightboxView() && this.lightboxFit() : this.showLayersDialog();
            this.chromelessResize && this.chromelessResize();
            this.editor.undoManager.clear();
            this.editor.modified = null != c ? c : !1;
            this.updateUi();
            window.self !== window.top && window.focus();
            null != this.format && this.format.refresh()
        }))
    };
    EditorUi.prototype.showLayersDialog = function() {
        1 < this.editor.graph.getModel().getChildCount(this.editor.graph.getModel().getRoot()) && (null == this.actions.layersWindow ? this.actions.get("layers").funct() : this.actions.layersWindow.window.setVisible(!0))
    };
    EditorUi.prototype.getPublicUrl = function(a, b) {
        null != a ? a.getPublicUrl(b) : b(null)
    };
    EditorUi.prototype.createLoadMessage = function(a) {
        var b = this.editor.graph;
        return {
            event: a,
            pageVisible: b.pageVisible,
            translate: b.view.translate,
            bounds: b.getGraphBounds(),
            currentPage: this.getSelectedPageIndex(),
            scale: b.view.scale,
            page: b.view.getBackgroundPageBounds()
        }
    };
    EditorUi.prototype.installMessageHandler = function(a) {
        var b = null,
            d = !1,
            c = !1,
            e = null,
            f = mxUtils.bind(this, function(a, b) {
                this.editor.modified && "0" != urlParams.modified ? null != urlParams.modified && this.editor.setStatus(mxUtils.htmlEntities(mxResources.get(urlParams.modified))) : this.editor.setStatus("")
            });
        this.editor.graph.model.addListener(mxEvent.CHANGE, f);
        mxEvent.addListener(window, "message", mxUtils.bind(this, function(f) {
            if (f.source == (window.opener || window.parent)) {
                var g = f.data,
                    m = mxUtils.bind(this, function(a) {
                        if (null != a && "function" === typeof a.charAt && "<" != a.charAt(0)) try {
                            "data:image/png;base64," == a.substring(0, 22) ? a = this.extractGraphModelFromPng(a) : "data:image/svg+xml;base64," == a.substring(0, 26) ? a = atob(a.substring(26)) : "data:image/svg+xml;utf8," == a.substring(0, 24) && (a = a.substring(24)), null != a && ("%" == a.charAt(0) ? a = decodeURIComponent(a) : "<" != a.charAt(0) && (a = Graph.decompress(a)))
                        } catch (P) {}
                        return a
                    });
                if ("json" == urlParams.proto) {
                    try {
                        g = JSON.parse(g)
                    } catch (T) {
                        g = null
                    }
                    try {
                        if (null == g) return;
                        if ("dialog" == g.action) {
                            this.showError(null != g.titleKey ? mxResources.get(g.titleKey) : g.title, null != g.messageKey ? mxResources.get(g.messageKey) : g.message, null != g.buttonKey ? mxResources.get(g.buttonKey) : g.button);
                            null != g.modified && (this.editor.modified = g.modified);
                            return
                        }
                        if ("layout" == g.action) {
                            this.executeLayoutList(g.layouts);
                            return
                        }
                        if ("prompt" == g.action) {
                            this.spinner.stop();
                            var l = new FilenameDialog(this, g.defaultValue || "", null != g.okKey ? mxResources.get(g.okKey) : g.ok, function(a) {
                                null != a ? k.postMessage(JSON.stringify({
                                    event: "prompt",
                                    value: a,
                                    message: g
                                }), "*") : k.postMessage(JSON.stringify({
                                    event: "prompt-cancel",
                                    message: g
                                }), "*")
                            }, null != g.titleKey ? mxResources.get(g.titleKey) : g.title);
                            this.showDialog(l.container, 300, 80, !0, !1);
                            l.init();
                            return
                        }
                        if ("draft" == g.action) {
                            var n = m(g.xml);
                            this.spinner.stop();
                            l = new DraftDialog(this, mxResources.get("draftFound", [g.name || this.defaultFilename]), n, mxUtils.bind(this, function() {
                                this.hideDialog();
                                k.postMessage(JSON.stringify({
                                    event: "draft",
                                    result: "edit",
                                    message: g
                                }), "*")
                            }), mxUtils.bind(this, function() {
                                this.hideDialog();
                                k.postMessage(JSON.stringify({
                                    event: "draft",
                                    result: "discard",
                                    message: g
                                }), "*")
                            }), g.editKey ? mxResources.get(g.editKey) : null, g.discardKey ? mxResources.get(g.discardKey) : null, g.ignore ? mxUtils.bind(this, function() {
                                this.hideDialog();
                                k.postMessage(JSON.stringify({
                                    event: "draft",
                                    result: "ignore",
                                    message: g
                                }), "*")
                            }) : null);
                            this.showDialog(l.container, 640, 480, !0, !1, mxUtils.bind(this, function(a) {
                                a && this.actions.get("exit").funct()
                            }));
                            try {
                                l.init()
                            } catch (T) {
                                k.postMessage(JSON.stringify({
                                    event: "draft",
                                    error: T.toString(),
                                    message: g
                                }), "*")
                            }
                            return
                        }
                        if ("template" == g.action) {
                            this.spinner.stop();
                            var p = 1 == g.enableRecent,
                                t = 1 == g.enableSearch,
                                q = 1 == g.enableCustomTemp,
                                l = new NewDialog(this, !1, null != g.callback, mxUtils.bind(this, function(b, d) {
                                    b = b || this.emptyDiagramXml;
                                    null != g.callback ? k.postMessage(JSON.stringify({
                                        event: "template",
                                        xml: b,
                                        blank: b == this.emptyDiagramXml,
                                        name: d,
                                        message: g
                                    }), "*") : (a(b, f, b != this.emptyDiagramXml), this.editor.modified || this.editor.setStatus(""))
                                }), null, null, null, null, null, null, null, p ? mxUtils.bind(this, function(a) {
                                    this.remoteInvoke("getRecentDiagrams", null, null, a, function() {
                                        a(null, "Network Error!")
                                    })
                                }) : null, t ? mxUtils.bind(this, function(a, b) {
                                    this.remoteInvoke("searchDiagrams", [a], null, b, function() {
                                        b(null, "Network Error!")
                                    })
                                }) : null, mxUtils.bind(this, function(a, b, d) {
                                    k.postMessage(JSON.stringify({
                                        event: "template",
                                        docUrl: a,
                                        info: b,
                                        name: d
                                    }), "*")
                                }), null, null, q ? mxUtils.bind(this, function(a) {
                                    this.remoteInvoke("getCustomTemplates", null, null, a, function() {
                                        a({}, 0)
                                    })
                                }) : null);
                            this.showDialog(l.container, 620, 440, !0, !1, mxUtils.bind(this, function(a) {
                                a && this.actions.get("exit").funct()
                            }));
                            l.init();
                            return
                        }
                        if ("textContent" == g.action) {
                            var v = this.getDiagramTextContent();
                            k.postMessage(JSON.stringify({
                                event: "textContent",
                                data: v,
                                message: g
                            }), "*");
                            return
                        }
                        if ("status" == g.action) {
                            null != g.messageKey ? this.editor.setStatus(mxUtils.htmlEntities(mxResources.get(g.messageKey))) : null != g.message && this.editor.setStatus(mxUtils.htmlEntities(g.message));
                            null != g.modified && (this.editor.modified = g.modified);
                            return
                        }
                        if ("spinner" == g.action) {
                            var u = null != g.messageKey ? mxResources.get(g.messageKey) : g.message;
                            null == g.show || g.show ? this.spinner.spin(document.body, u) : this.spinner.stop();
                            return
                        }
                        if ("export" == g.action) {
                            if ("png" == g.format || "xmlpng" == g.format) {
                                if (null == g.spin && null == g.spinKey || this.spinner.spin(document.body, null != g.spinKey ? mxResources.get(g.spinKey) : g.spin)) {
                                    var H = null != g.xml ? g.xml : this.getFileData(!0);
                                    this.editor.graph.setEnabled(!1);
                                    var I = this.editor.graph,
                                        K = mxUtils.bind(this, function(a) {
                                            this.editor.graph.setEnabled(!0);
                                            this.spinner.stop();
                                            var b = this.createLoadMessage("export");
                                            b.format = g.format;
                                            b.message = g;
                                            b.data = a;
                                            b.xml = H;
                                            k.postMessage(JSON.stringify(b), "*")
                                        }),
                                        G = mxUtils.bind(this, function(a) {
                                            null == a && (a = Editor.blankImage);
                                            "xmlpng" == g.format && (a = Editor.writeGraphModelToPng(a, "tEXt", "mxfile", encodeURIComponent(H)));
                                            I != this.editor.graph && I.container.parentNode.removeChild(I.container);
                                            K(a)
                                        }),
                                        L = g.pageId || (null != this.pages ? this.pages[0].getId() : null);
                                    if (this.isExportToCanvas()) {
                                        if (null != this.pages && this.currentPage.getId() != L) {
                                            for (var O = I.getGlobalVariable, I = this.createTemporaryGraph(I.getStylesheet()),
                                                    J, S = 0; S < this.pages.length; S++)
                                                if (this.pages[S].getId() == L) {
                                                    J = this.updatePageRoot(this.pages[S]);
                                                    break
                                                }
                                            I.getGlobalVariable = function(a) {
                                                return "page" == a ? J.getName() : "pagenumber" == a ? 1 : O.apply(this, arguments)
                                            };
                                            document.body.appendChild(I.container);
                                            I.model.setRoot(J.root)
                                        }
                                        if (null != g.layerIds) {
                                            for (var W = I.model, aa = W.getChildCells(W.getRoot()), l = {}, S = 0; S < g.layerIds.length; S++) l[g.layerIds[S]] = !0;
                                            for (S = 0; S < aa.length; S++) W.setVisible(aa[S], l[aa[S].id] || !1)
                                        }
                                        this.editor.exportToCanvas(mxUtils.bind(this, function(a) {
                                            G(a.toDataURL("image/png"))
                                        }), null, null, null, mxUtils.bind(this, function() {
                                            G(null)
                                        }), null, null, g.scale, null, null, null, I)
                                    } else(new mxXmlRequest(EXPORT_URL, "format=png&embedXml=" + ("xmlpng" == g.format ? "1" : "0") + (null != L ? "&pageId=" + L : "") + (null != g.layerIds ? "&extras=" + encodeURIComponent(JSON.stringify({
                                        layerIds: g.layerIds
                                    })) : "") + (null != g.scale ? "&scale=" + g.scale : "") + "&base64=1&xml=" + encodeURIComponent(H))).send(mxUtils.bind(this, function(a) {
                                        200 <= a.getStatus() && 299 >= a.getStatus() ? K("data:image/png;base64," + a.getText()) : G(null)
                                    }), mxUtils.bind(this, function() {
                                        G(null)
                                    }))
                                }
                            } else {
                                null != g.xml && 0 < g.xml.length && this.setFileData(g.xml);
                                u = this.createLoadMessage("export");
                                u.message = g;
                                if ("html2" == g.format || "html" == g.format && ("0" != urlParams.pages || null != this.pages && 1 < this.pages.length)) {
                                    var ca = this.getXmlFileData();
                                    u.xml = mxUtils.getXml(ca);
                                    u.data = this.getFileData(null, null, !0, null, null, null, ca);
                                    u.format = g.format
                                } else if ("html" == g.format) H = this.editor.getGraphXml(), u.data = this.getHtml(H, this.editor.graph), u.xml = mxUtils.getXml(H), u.format = g.format;
                                else {
                                    mxSvgCanvas2D.prototype.foAltText = null;
                                    var Y = this.editor.graph.background;
                                    Y == mxConstants.NONE && (Y = null);
                                    u.xml = this.getFileData(!0, null, null, null, null, null, null, null, null, !1);
                                    u.format = "svg";
                                    var V = mxUtils.bind(this, function(a) {
                                        this.editor.graph.setEnabled(!0);
                                        this.spinner.stop();
                                        u.data = Editor.createSvgDataUri(a);
                                        k.postMessage(JSON.stringify(u), "*")
                                    });
                                    if ("xmlsvg" == g.format)(null == g.spin && null == g.spinKey || this.spinner.spin(document.body, null != g.spinKey ? mxResources.get(g.spinKey) : g.spin)) && this.getEmbeddedSvg(u.xml, this.editor.graph, null, !0, V, null, null, g.embedImages);
                                    else if (null == g.spin && null == g.spinKey || this.spinner.spin(document.body, null != g.spinKey ? mxResources.get(g.spinKey) : g.spin)) {
                                        this.editor.graph.setEnabled(!1);
                                        var U = this.editor.graph.getSvg(Y);
                                        this.embedFonts(U, mxUtils.bind(this, function(a) {
                                            g.embedImages || null == g.embedImages ? this.editor.convertImages(a, mxUtils.bind(this, function(a) {
                                                V(mxUtils.getXml(a))
                                            })) : V(mxUtils.getXml(a))
                                        }))
                                    }
                                    return
                                }
                                k.postMessage(JSON.stringify(u), "*")
                            }
                            return
                        }
                        if ("load" == g.action) c = 1 == g.autosave, this.hideDialog(),
                            null != g.modified && null == urlParams.modified && (urlParams.modified = g.modified), null != g.saveAndExit && null == urlParams.saveAndExit && (urlParams.saveAndExit = g.saveAndExit), null != g.noSaveBtn && null == urlParams.noSaveBtn && (urlParams.noSaveBtn = g.noSaveBtn), null != g.noExitBtn && null == urlParams.noExitBtn && (urlParams.noExitBtn = g.noExitBtn), null != g.title && null != this.buttonContainer && (n = document.createElement("span"), mxUtils.write(n, g.title), "atlas" == uiTheme ? (this.buttonContainer.style.paddingRight = "12px", this.buttonContainer.style.paddingTop = "6px", this.buttonContainer.style.right = "25px") : "min" != uiTheme && (this.buttonContainer.style.paddingRight = "38px", this.buttonContainer.style.paddingTop = "6px"), null != this.embedFilenameSpan && this.embedFilenameSpan.parentNode.removeChild(this.embedFilenameSpan), this.buttonContainer.appendChild(n), this.embedFilenameSpan = n), g = null != g.xmlpng ? this.extractGraphModelFromPng(g.xmlpng) : g.xml;
                        else {
                            if ("merge" == g.action) {
                                var Z = this.getCurrentFile();
                                null != Z && (n = m(g.xml), null != n && "" != n && Z.mergeFile(new LocalFile(this, n), function() {
                                    k.postMessage(JSON.stringify({
                                        event: "merge",
                                        message: g
                                    }), "*")
                                }, function(a) {
                                    k.postMessage(JSON.stringify({
                                        event: "merge",
                                        message: g,
                                        error: a
                                    }), "*")
                                }))
                            } else "remoteInvokeReady" == g.action ? this.handleRemoteInvokeReady(k) : "remoteInvoke" == g.action ? this.handleRemoteInvoke(g, f.origin) : "remoteInvokeResponse" == g.action ? this.handleRemoteInvokeResponse(g) : k.postMessage(JSON.stringify({
                                error: "unknownMessage",
                                data: JSON.stringify(g)
                            }), "*");
                            return
                        }
                    } catch (T) {
                        this.handleError(T)
                    }
                }
                var ba = mxUtils.bind(this, function(f, g) {
                    d = !0;
                    try {
                        a(f, g)
                    } catch (ga) {
                        this.handleError(ga)
                    }
                    d = !1;
                    null != urlParams.modified && this.editor.setStatus("");
                    var l = mxUtils.bind(this, function() {
                        return "0" != urlParams.pages || null != this.pages && 1 < this.pages.length ? this.getFileData(!0) : mxUtils.getXml(this.editor.getGraphXml())
                    });
                    e = l();
                    c && null == b && (b = mxUtils.bind(this, function(a, b) {
                        var c = l();
                        if (c != e && !d) {
                            var f = this.createLoadMessage("autosave");
                            f.xml = c;
                            c = JSON.stringify(f);
                            (window.opener || window.parent).postMessage(c, "*")
                        }
                        e = c
                    }), this.editor.graph.model.addListener(mxEvent.CHANGE, b), this.editor.graph.addListener("gridSizeChanged", b), this.editor.graph.addListener("shadowVisibleChanged", b), this.addListener("pageFormatChanged", b), this.addListener("pageScaleChanged", b), this.addListener("backgroundColorChanged", b), this.addListener("backgroundImageChanged", b), this.addListener("foldingEnabledChanged", b), this.addListener("mathEnabledChanged", b), this.addListener("gridEnabledChanged", b), this.addListener("guidesEnabledChanged", b), this.addListener("pageViewChanged", b));
                    if ("1" == urlParams.returnbounds || "json" == urlParams.proto) {
                        var m = this.createLoadMessage("load");
                        m.xml = f;
                        k.postMessage(JSON.stringify(m), "*")
                    }
                });
                null != g && "function" === typeof g.substring && "data:application/vnd.visio;base64," == g.substring(0, 34) ? (m = "0M8R4KGxGuE" == g.substring(34, 45) ? "raw.vsd" : "raw.vsdx", this.importVisio(this.base64ToBlob(g.substring(g.indexOf(",") + 1)), function(a) {
                    ba(a, f)
                }, mxUtils.bind(this, function(a) {
                    this.handleError(a)
                }), m)) : null != g && "function" === typeof g.substring && !this.isOffline() && (new XMLHttpRequest).upload && this.isRemoteFileFormat(g, "") ? this.parseFile(new Blob([g], {
                    type: "application/octet-stream"
                }), mxUtils.bind(this, function(a) {
                    4 == a.readyState && 200 <= a.status && 299 >= a.status && "<mxGraphModel" == a.responseText.substring(0, 13) && ba(a.responseText, f)
                }), "") : null != g && "function" === typeof g.substring && this.isLucidChartData(g) ? this.convertLucidChart(g, mxUtils.bind(this, function(a) {
                    ba(a)
                }), mxUtils.bind(this, function(a) {
                    this.handleError(a)
                })) : (g = m(g), ba(g, f))
            }
        }));
        var k = window.opener || window.parent,
            f = "json" == urlParams.proto ? JSON.stringify({
                event: "init"
            }) : urlParams.ready || "ready";
        k.postMessage(f, "*");
        if ("json" == urlParams.proto) {
            var l = this.editor.graph.openLink;
            this.editor.graph.openLink = function(a, b, d) {
                l.apply(this, arguments);
                k.postMessage(JSON.stringify({
                    event: "openLink",
                    href: a,
                    target: b,
                    allowOpener: d
                }), "*")
            }
        }
    };
    EditorUi.prototype.addEmbedButtons = function() {
        if (null != this.menubar) {
            var a = document.createElement("div");
            a.style.display = "inline-block";
            a.style.position = "absolute";
            a.style.paddingTop = "atlas" == uiTheme ? "2px" : "0px";
            a.style.paddingLeft = "8px";
            a.style.paddingBottom = "2px";
            var b = document.createElement("button");
            b.className = "geBigButton";
            var c = b;
            "1" == urlParams.noSaveBtn ? "0" != urlParams.saveAndExit && (mxUtils.write(b, mxResources.get("saveAndExit")), b.setAttribute("title", mxResources.get("saveAndExit")), mxEvent.addListener(b, "click", mxUtils.bind(this, function() {
                this.actions.get("saveAndExit").funct()
            })), a.appendChild(b)) : (mxUtils.write(b, mxResources.get("save")), b.setAttribute("title", mxResources.get("save") + " (" + Editor.ctrlKey + "+S)"), mxEvent.addListener(b, "click", mxUtils.bind(this, function() {
                this.actions.get("save").funct()
            })), a.appendChild(b), "1" == urlParams.saveAndExit && (b = document.createElement("a"), mxUtils.write(b, mxResources.get("saveAndExit")), b.setAttribute("title", mxResources.get("saveAndExit")), b.className = "geBigButton geBigStandardButton", b.style.marginLeft = "6px", mxEvent.addListener(b, "click", mxUtils.bind(this, function() {
                this.actions.get("saveAndExit").funct()
            })), a.appendChild(b), c = b));
            "1" != urlParams.noExitBtn && (b = document.createElement("a"), mxUtils.write(b, mxResources.get("exit")), b.setAttribute("title", mxResources.get("exit")), b.className = "geBigButton geBigStandardButton", b.style.marginLeft = "6px", mxEvent.addListener(b, "click", mxUtils.bind(this, function() {
                this.actions.get("exit").funct()
            })), a.appendChild(b), c = b);
            c.style.marginRight = "20px";
            this.toolbar.container.appendChild(a);
            this.toolbar.staticElements.push(a);
            a.style.right = "atlas" != uiTheme ? "52px" : "42px"
        }
    };
    EditorUi.prototype.showImportCsvDialog = function() {
        null == this.importCsvDialog && (this.importCsvDialog = new TextareaDialog(this, mxResources.get("csv") + ":", Editor.defaultCsvValue, mxUtils.bind(this, function(a) {
            this.importCsv(a)
        }), null, null, 620, 430, null, !0, !0, mxResources.get("import"), this.isOffline() ? null : "https://drawio-app.com/import-from-csv-to-drawio/"));
        this.showDialog(this.importCsvDialog.container, 640, 520, !0, !0, null, null, null, null, !0);
        this.importCsvDialog.init()
    };
    EditorUi.prototype.executeLayoutList = function(a, b) {
        for (var d = this.editor.graph, c = d.getSelectionCells(), e = 0; e < a.length; e++) {
            var f = new window[a[e].layout](d);
            if (null != a[e].config)
                for (var g in a[e].config) f[g] = a[e].config[g];
            this.executeLayout(function() {
                f.execute(d.getDefaultParent(), 0 == c.length ? null : c)
            }, e == a.length - 1, b)
        }
    };
    EditorUi.prototype.importCsv = function(a, b) {
        try {
            var d = a.split("\n"),
                c = [],
                e = [],
                f = {};
            if (0 < d.length) {
                var g = {},
                    k = null,
                    l = null,
                    x = null,
                    y = null,
                    A = null,
                    z = null,
                    B = null,
                    C = null,
                    D = "",
                    E = "auto",
                    F = "auto",
                    H = null,
                    I = null,
                    K = 40,
                    G = 40,
                    L = 100,
                    O = 0,
                    J = this.editor.graph;
                J.getGraphBounds();
                for (var S = function() {
                            null != b ? b(ka) : (J.setSelectionCells(ka), J.scrollCellToVisible(J.getSelectionCell()))
                        },
                        W = J.getFreeInsertPoint(), aa = W.x, ca = W.y, W = ca, Y = null, V = "auto", C = null, U = [], Z = null, ba = null, T = 0; T < d.length && "#" == d[T].charAt(0);) {
                    a = d[T];
                    for (T++; T < d.length && "\\" == a.charAt(a.length - 1) && "#" == d[T].charAt(0);) a = a.substring(0, a.length - 1) + mxUtils.trim(d[T].substring(1)), T++;
                    if ("#" != a.charAt(1)) {
                        var P = a.indexOf(":");
                        if (0 < P) {
                            var Q = mxUtils.trim(a.substring(1, P)),
                                M = mxUtils.trim(a.substring(P + 1));
                            "label" == Q ? Y = J.sanitizeHtml(M) : "labelname" == Q && 0 < M.length && "-" != M ? y = M : "labels" == Q && 0 < M.length && "-" != M ? A = JSON.parse(M) : "style" == Q ? k = M : "parentstyle" == Q ? z = M : "stylename" == Q && 0 < M.length && "-" != M ? x = M : "styles" == Q && 0 < M.length && "-" != M ? l = JSON.parse(M) : "identity" == Q && 0 < M.length && "-" != M ? B = M : "parent" == Q && 0 < M.length && "-" != M ? C = M : "namespace" == Q && 0 < M.length && "-" != M ? D = M : "width" == Q ? E = M : "height" == Q ? F = M : "left" == Q && 0 < M.length ? H = M : "top" == Q && 0 < M.length ? I = M : "ignore" == Q ? ba = M.split(",") : "connect" == Q ? U.push(JSON.parse(M)) : "link" == Q ? Z = M : "padding" == Q ? O = parseFloat(M) : "edgespacing" == Q ? K = parseFloat(M) : "nodespacing" == Q ? G = parseFloat(M) : "levelspacing" == Q ? L = parseFloat(M) : "layout" == Q && (V = M)
                        }
                    }
                }
                if (null == d[T]) throw Error(mxResources.get("invalidOrMissingFile"));
                for (var ga = this.editor.csvToArray(d[T]), Q = P = null, M = [], R = 0; R < ga.length; R++) B == ga[R] && (P = R), C == ga[R] && (Q = R), M.push(mxUtils.trim(ga[R]).replace(/[^a-z0-9]+/ig, "_").replace(/^\d+/, "").replace(/_+$/, ""));
                null == Y && (Y = "%" + M[0] + "%");
                if (null != U)
                    for (var X = 0; X < U.length; X++) null == g[U[X].to] && (g[U[X].to] = {});
                B = [];
                for (R = T + 1; R < d.length; R++) {
                    var da = this.editor.csvToArray(d[R]);
                    if (null == da) {
                        var sa = 40 < d[R].length ? d[R].substring(0, 40) + "..." : d[R];
                        throw Error(sa + " (" + R + "):\n" + mxResources.get("containsValidationErrors"));
                    }
                    0 < da.length && B.push(da)
                }
                J.model.beginUpdate();
                try {
                    for (R = 0; R < B.length; R++) {
                        var da = B[R],
                            N = null,
                            ia = null != P ? D + da[P] : null;
                        null != ia && (N = J.model.getCell(ia));
                        var d = null != N,
                            fa = new mxCell(Y, new mxGeometry(aa, W, 0, 0), k || "whiteSpace=wrap;html=1;");
                        fa.vertex = !0;
                        fa.id = ia;
                        for (var ea = 0; ea < da.length; ea++) J.setAttributeForCell(fa, M[ea], da[ea]);
                        if (null != y && null != A) {
                            var ja = A[fa.getAttribute(y)];
                            null != ja && J.labelChanged(fa, ja)
                        }
                        if (null != x && null != l) {
                            var oa = l[fa.getAttribute(x)];
                            null != oa && (fa.style = oa)
                        }
                        J.setAttributeForCell(fa, "placeholders", "1");
                        fa.style = J.replacePlaceholders(fa, fa.style);
                        d && (J.model.setGeometry(N, fa.geometry), J.model.setStyle(N, fa.style), 0 > mxUtils.indexOf(e, N) && e.push(N));
                        N = fa;
                        if (!d)
                            for (X = 0; X < U.length; X++) g[U[X].to][N.getAttribute(U[X].to)] = N;
                        null != Z && "link" != Z && (J.setLinkForCell(N, N.getAttribute(Z)), J.setAttributeForCell(N, Z, null));
                        J.fireEvent(new mxEventObject("cellsInserted", "cells", [N]));
                        var ha = this.editor.graph.getPreferredSizeForCell(N);
                        N.vertex && (null != H && null != N.getAttribute(H) && (N.geometry.x = aa + parseFloat(N.getAttribute(H))), null != I && null != N.getAttribute(I) && (N.geometry.y = ca + parseFloat(N.getAttribute(I))), "@" == E.charAt(0) && null != N.getAttribute(E.substring(1)) ? N.geometry.width = parseFloat(N.getAttribute(E.substring(1))) : N.geometry.width = "auto" == E ? ha.width + O : parseFloat(E), "@" == F.charAt(0) && null != N.getAttribute(F.substring(1)) ? N.geometry.height = parseFloat(N.getAttribute(F.substring(1))) : N.geometry.height = "auto" == F ? ha.height + O : parseFloat(F), W += N.geometry.height + G);
                        d ? (null == f[ia] && (f[ia] = []), f[ia].push(N)) : (C = null != Q ? J.model.getCell(D + da[Q]) : null, c.push(N), null != C ? (C.style = J.replacePlaceholders(C, z), J.addCell(N, C)) : e.push(J.addCell(N)))
                    }
                    for (var ma = e.slice(), ka = e.slice(), X = 0; X < U.length; X++)
                        for (var ua = U[X], R = 0; R < c.length; R++) {
                            var N = c[R],
                                va = mxUtils.bind(this, function(a, b, d) {
                                    var c = b.getAttribute(d.from);
                                    if (null != c && (J.setAttributeForCell(b, d.from, null), "" != c))
                                        for (var c = c.split(","), e = 0; e < c.length; e++) {
                                            var f = g[d.to][c[e]];
                                            if (null != f) {
                                                var k = d.label;
                                                null != d.fromlabel && (k = (b.getAttribute(d.fromlabel) || "") + (k || ""));
                                                null != d.tolabel && (k = (k || "") + (f.getAttribute(d.tolabel) || ""));
                                                var l = "target" == d.placeholders == !d.invert ? f : a,
                                                    l = null != d.style ? J.replacePlaceholders(l, d.style) : J.createCurrentEdgeStyle(),
                                                    k = J.insertEdge(null, null, k || "", d.invert ? f : a, d.invert ? a : f, l);
                                                if (null != d.labels)
                                                    for (l = 0; l < d.labels.length; l++) {
                                                        var m = d.labels[l],
                                                            n = new mxCell(m.label || l, new mxGeometry(null != m.x ? m.x : 0, null != m.y ? m.y : 0, 0, 0), "resizable=0;html=1;");
                                                        n.vertex = !0;
                                                        n.connectable = !1;
                                                        n.geometry.relative = !0;
                                                        if (null != m.dx || null != m.dy) n.geometry.offset = new mxPoint(null != m.dx ? m.dx : 0, null != m.dy ? m.dy : 0);
                                                        k.insert(n)
                                                    }
                                                ka.push(k);
                                                mxUtils.remove(d.invert ? a : f, ma)
                                            }
                                        }
                                });
                            va(N, N, ua);
                            if (null != f[N.id])
                                for (ea = 0; ea < f[N.id].length; ea++) va(N, f[N.id][ea], ua)
                        }
                    if (null != ba)
                        for (R = 0; R < c.length; R++)
                            for (N = c[R], ea = 0; ea < ba.length; ea++) J.setAttributeForCell(N, mxUtils.trim(ba[ea]), null);
                    if (0 < e.length) {
                        var la = new mxParallelEdgeLayout(J);
                        la.spacing = K;
                        var ta = function() {
                            0 < la.spacing && la.execute(J.getDefaultParent());
                            for (var a = 0; a < e.length; a++) {
                                var b = J.getCellGeometry(e[a]);
                                b.x = Math.round(J.snap(b.x));
                                b.y = Math.round(J.snap(b.y));
                                "auto" == E && (b.width = Math.round(J.snap(b.width)));
                                "auto" == F && (b.height = Math.round(J.snap(b.height)))
                            }
                        };
                        if ("[" == V.charAt(0)) {
                            var wa = S;
                            J.view.validate();
                            this.executeLayoutList(JSON.parse(V), function() {
                                ta();
                                wa()
                            });
                            S = null
                        } else if ("circle" == V) {
                            var na = new mxCircleLayout(J);
                            na.resetEdges = !1;
                            var xa = na.isVertexIgnored;
                            na.isVertexIgnored = function(a) {
                                return xa.apply(this, arguments) || 0 > mxUtils.indexOf(e, a)
                            };
                            this.executeLayout(function() {
                                na.execute(J.getDefaultParent());
                                ta()
                            }, !0, S);
                            S = null
                        } else if ("horizontaltree" == V || "verticaltree" == V || "auto" == V && ka.length == 2 * e.length - 1 && 1 == ma.length) {
                            J.view.validate();
                            var ra = new mxCompactTreeLayout(J, "horizontaltree" == V);
                            ra.levelDistance = G;
                            ra.edgeRouting = !1;
                            ra.resetEdges = !1;
                            this.executeLayout(function() {
                                ra.execute(J.getDefaultParent(), 0 < ma.length ? ma[0] : null)
                            }, !0, S);
                            S = null
                        } else if ("horizontalflow" == V || "verticalflow" == V || "auto" == V && 1 == ma.length) {
                            J.view.validate();
                            var pa = new mxHierarchicalLayout(J, "horizontalflow" == V ? mxConstants.DIRECTION_WEST : mxConstants.DIRECTION_NORTH);
                            pa.intraCellSpacing = G;
                            pa.parallelEdgeSpacing = K;
                            pa.interRankCellSpacing = L;
                            pa.disableEdgeStyle = !1;
                            this.executeLayout(function() {
                                pa.execute(J.getDefaultParent(), ka);
                                J.moveCells(ka, aa, ca)
                            }, !0, S);
                            S = null
                        } else if ("organic" == V || "auto" == V && ka.length > e.length) {
                            J.view.validate();
                            var qa = new mxFastOrganicLayout(J);
                            qa.forceConstant = 3 * G;
                            qa.resetEdges = !1;
                            var ya = qa.isVertexIgnored;
                            qa.isVertexIgnored = function(a) {
                                return ya.apply(this, arguments) || 0 > mxUtils.indexOf(e, a)
                            };
                            la = new mxParallelEdgeLayout(J);
                            la.spacing = K;
                            this.executeLayout(function() {
                                qa.execute(J.getDefaultParent());
                                ta()
                            }, !0, S);
                            S = null
                        }
                    }
                    this.hideDialog()
                } finally {
                    J.model.endUpdate()
                }
                null != S && S()
            }
        } catch (za) {
            this.handleError(za)
        }
    };
    EditorUi.prototype.getSearch = function(a) {
        var b = "";
        if ("1" != urlParams.offline && "1" != urlParams.demo && null != a && 0 < window.location.search.length) {
            var d = "?",
                c;
            for (c in urlParams) 0 > mxUtils.indexOf(a, c) && null != urlParams[c] && (b += d + c + "=" + urlParams[c], d = "&")
        } else b = window.location.search;
        return b
    };
    EditorUi.prototype.getUrl = function(a) {
        a = null != a ? a : window.location.pathname;
        var b = 0 < a.indexOf("?") ? 1 : 0;
        if ("1" == urlParams.offline) a += window.location.search;
        else {
            var d = "tmp libs clibs state fileId code share notitle data url embed client create title splash".split(" "),
                c;
            for (c in urlParams) 0 > mxUtils.indexOf(d, c) && (a = 0 == b ? a + "?" : a + "&", null != urlParams[c] && (a += c + "=" + urlParams[c], b++))
        }
        return a
    };
    EditorUi.prototype.showLinkDialog = function(a, b, c) {
        a = new LinkDialog(this, a, b, c, !0);
        this.showDialog(a.container, 560, 130, !0, !0);
        a.init()
    };
    var k = EditorUi.prototype.createOutline;
    EditorUi.prototype.createOutline = function(a) {
        var b = k.apply(this, arguments),
            c = this.editor.graph,
            d = b.getSourceGraphBounds;
        b.getSourceGraphBounds = function() {
            if (mxUtils.hasScrollbars(c.container) && c.pageVisible && null != this.source.minimumGraphSize) {
                var a = this.source.getPagePadding(),
                    b = this.source.view.scale;
                return new mxRectangle(0, 0, Math.ceil(this.source.minimumGraphSize.width - 2 * a.x / b), Math.ceil(this.source.minimumGraphSize.height - 2 * a.y / b))
            }
            return d.apply(this, arguments)
        };
        var e = b.getSourceContainerSize;
        b.getSourceContainerSize = function() {
            if (mxUtils.hasScrollbars(c.container) && null != this.source.minimumGraphSize) {
                var a = this.source.getPagePadding(),
                    b = this.source.view.scale;
                return new mxRectangle(0, 0, Math.ceil(this.source.minimumGraphSize.width * b - 2 * a.x), Math.ceil(this.source.minimumGraphSize.height * b - 2 * a.y))
            }
            return e.apply(this, arguments)
        };
        b.getOutlineOffset = function(a) {
            if (mxUtils.hasScrollbars(c.container) && null != this.source.minimumGraphSize) {
                var d = this.source.getPagePadding();
                return new mxPoint(Math.round(Math.max(0, (b.outline.container.clientWidth / a - (this.source.minimumGraphSize.width - 2 * d.x)) / 2) - d.x), Math.round(Math.max(0, (b.outline.container.clientHeight / a - (this.source.minimumGraphSize.height - 2 * d.y)) / 2) - d.y - 5 / a))
            }
            return new mxPoint(8 / a, 8 / a)
        };
        var f = b.init;
        b.init = function() {
            f.apply(this, arguments);
            b.outline.view.getBackgroundPageBounds = function() {
                var a = c.getPageLayout(),
                    b = c.getPageSize();
                return new mxRectangle(this.scale * (this.translate.x + a.x * b.width), this.scale * (this.translate.y + a.y * b.height), this.scale * a.width * b.width, this.scale * a.height * b.height)
            };
            b.outline.view.validateBackgroundPage()
        };
        this.editor.addListener("pageSelected", function(a, c) {
            var d = c.getProperty("change"),
                e = b.source,
                f = b.outline;
            f.pageScale = e.pageScale;
            f.pageFormat = e.pageFormat;
            f.background = e.background;
            f.pageVisible = e.pageVisible;
            f.background = e.background;
            var g = mxUtils.getCurrentStyle(e.container);
            f.container.style.backgroundColor = g.backgroundColor;
            null != e.view.backgroundPageShape && null != f.view.backgroundPageShape && (f.view.backgroundPageShape.fill = e.view.backgroundPageShape.fill);
            b.outline.view.clear(d.previousPage.root, !0);
            b.outline.view.validate()
        });
        return b
    };
    EditorUi.prototype.getServiceCount = function(a) {
        var b = 1;
        null == this.drive && "function" !== typeof window.DriveClient || b++;
        null == this.dropbox && "function" !== typeof window.DropboxClient || b++;
        null == this.oneDrive && "function" !== typeof window.OneDriveClient || b++;
        null != this.gitHub && b++;
        null != this.gitLab && b++;
        a && isLocalStorage && "1" == urlParams.browser && b++;
        return b
    };
    EditorUi.prototype.updateUi = function() {
        this.updateButtonContainer();
        this.updateActionStates();
        var a = this.getCurrentFile(),
            b = null != a || "1" == urlParams.embed && this.editor.graph.isEnabled();
        this.menus.get("viewPanels").setEnabled(b);
        this.menus.get("viewZoom").setEnabled(b);
        var c = ("1" != urlParams.embed || !this.editor.graph.isEnabled()) && (null == a || a.isRestricted());
        this.actions.get("makeCopy").setEnabled(!c);
        this.actions.get("print").setEnabled(!c);
        this.menus.get("exportAs").setEnabled(!c);
        this.menus.get("embed").setEnabled(!c);
        c = "1" != urlParams.embed || this.editor.graph.isEnabled();
        this.menus.get("extras").setEnabled(c);
        Editor.enableCustomLibraries && (this.menus.get("openLibraryFrom").setEnabled(c), this.menus.get("newLibrary").setEnabled(c));
        a = "1" == urlParams.embed && this.editor.graph.isEnabled() || null != a && a.isEditable();
        this.actions.get("image").setEnabled(b);
        this.actions.get("zoomIn").setEnabled(b);
        this.actions.get("zoomOut").setEnabled(b);
        this.actions.get("resetView").setEnabled(b);
        this.actions.get("undo").setEnabled(this.canUndo() && a);
        this.actions.get("redo").setEnabled(this.canRedo() && a);
        this.menus.get("edit").setEnabled(b);
        this.menus.get("view").setEnabled(b);
        this.menus.get("importFrom").setEnabled(a);
        this.menus.get("arrange").setEnabled(a);
        null != this.toolbar && (null != this.toolbar.edgeShapeMenu && this.toolbar.edgeShapeMenu.setEnabled(a), null != this.toolbar.edgeStyleMenu && this.toolbar.edgeStyleMenu.setEnabled(a));
        this.updateUserElement()
    };
    EditorUi.prototype.updateButtonContainer = function() {};
    EditorUi.prototype.updateUserElement = function() {};
    EditorUi.prototype.scheduleSanityCheck = function() {};
    EditorUi.prototype.stopSanityCheck = function() {};
    EditorUi.prototype.isDiagramActive = function() {
        var a = this.getCurrentFile();
        return null != a && a.isEditable() || "1" == urlParams.embed && this.editor.graph.isEnabled()
    };
    var f = EditorUi.prototype.updateActionStates;
    EditorUi.prototype.updateActionStates = function() {
        f.apply(this, arguments);
        var a = this.editor.graph,
            b = this.isDiagramActive(),
            c = this.getCurrentFile();
        this.actions.get("setProblemSolvingLayerStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setInformationLayerStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setPhysicalLayerStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setDPBoxStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setCAIBoxStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setGHBoxStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setESIBoxStyle").setEnabled(b && !a.isSelectionEmpty());
        this.actions.get("setEXBoxStyle").setEnabled(b && !a.isSelectionEmpty());
    };
    var l = EditorUi.prototype.destroy;
    EditorUi.prototype.destroy = function() {
        null != this.exportDialog && (this.exportDialog.parentNode.removeChild(this.exportDialog), this.exportDialog = null);
        l.apply(this, arguments)
    };
    null != window.ExportDialog && (ExportDialog.showXmlOption = !1, ExportDialog.showGifOption = !1, ExportDialog.exportFile = function(a, b, c, e, f, k, l) {
        var d = a.editor.graph;
        if ("xml" == c) a.hideDialog(), a.saveData(b, "xml", mxUtils.getXml(a.editor.getGraphXml()), "text/xml");
        else if ("svg" == c) a.hideDialog(), a.saveData(b, "svg", mxUtils.getXml(d.getSvg(e, f, k)), "image/svg+xml");
        else {
            var g = a.getFileData(!0, null, null, null, null, !0),
                m = d.getGraphBounds(),
                n = Math.floor(m.width * f / d.view.scale),
                p = Math.floor(m.height * f / d.view.scale);
            if (g.length <= MAX_REQUEST_SIZE && n * p < MAX_AREA)
                if (a.hideDialog(), "png" != c && "jpg" != c && "jpeg" != c || !a.isExportToCanvas()) {
                    var z = {
                        globalVars: d.getExportVariables()
                    };
                    a.saveRequest(b, c, function(a, b) {
                        return new mxXmlRequest(EXPORT_URL, "format=" + c + "&base64=" + (b || "0") + (null != a ? "&filename=" + encodeURIComponent(a) : "") + "&extras=" + encodeURIComponent(JSON.stringify(z)) + (0 < l ? "&dpi=" + l : "") + "&bg=" + (null != e ? e : "none") + "&w=" + n + "&h=" + p + "&border=" + k + "&xml=" + encodeURIComponent(g))
                    })
                } else "png" == c ? a.exportImage(f, null == e || "none" == e, !0, !1, !1, k, !0, !1, null, null, l) : a.exportImage(f, !1, !0, !1, !1, k, !0, !1, "jpeg");
            else mxUtils.alert(mxResources.get("drawingTooLarge"))
        }
    });
    EditorUi.prototype.getDiagramTextContent = function() {
        this.editor.graph.setEnabled(!1);
        var a = this.editor.graph,
            b = "";
        if (null != this.pages)
            for (var c = 0; c < this.pages.length; c++) {
                var e = a;
                this.currentPage != this.pages[c] && (e = this.createTemporaryGraph(a.getStylesheet()), this.updatePageRoot(this.pages[c]), e.model.setRoot(this.pages[c].root));
                b += this.pages[c].getName() + " " + e.getIndexableText() + " "
            } else b = a.getIndexableText();
        this.editor.graph.setEnabled(!0);
        return b
    };
    EditorUi.prototype.showRemotelyStoredLibrary = function(a) {
        var b = {},
            c = document.createElement("div");
        c.style.whiteSpace = "nowrap";
        var d = document.createElement("h3");
        mxUtils.write(d, mxUtils.htmlEntities(a));
        d.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:12px";
        c.appendChild(d);
        var e = document.createElement("div");
        e.style.cssText = "border:1px solid lightGray;overflow: auto;height:300px";
        e.innerHTML = '<div style="text-align:center;padding:8px;"><img src="' + IMAGE_PATH + '/spin.gif"></div>';
        var f = {};
        try {
            var k = mxSettings.getCustomLibraries();
            for (a = 0; a < k.length; a++) {
                var l = k[a];
                if ("R" == l.substring(0, 1)) {
                    var v = JSON.parse(decodeURIComponent(l.substring(1)));
                    f[v[0]] = {
                        id: v[0],
                        title: v[1],
                        downloadUrl: v[2]
                    }
                }
            }
        } catch (x) {}
        this.remoteInvoke("getCustomLibraries", null, null, function(a) {
            e.innerHTML = "";
            if (0 == a.length) e.innerHTML = '<div style="text-align:center;padding-top:20px;color:gray;">' + mxUtils.htmlEntities(mxResources.get("noLibraries")) + "</div>";
            else
                for (var c = 0; c < a.length; c++) {
                    var d = a[c];
                    f[d.id] && (b[d.id] = d);
                    var g = this.addCheckbox(e, d.title, f[d.id]);
                    (function(a, c) {
                        mxEvent.addListener(c, "change", function() {
                            this.checked ? b[a.id] = a : delete b[a.id]
                        })
                    })(d, g)
                }
        }, mxUtils.bind(this, function(a) {
            e.innerHTML = "";
            var b = document.createElement("div");
            b.style.padding = "8px";
            b.style.textAlign = "center";
            mxUtils.write(b, mxResources.get("error") + ": ");
            mxUtils.write(b, null != a && null != a.message ? a.message : mxResources.get("unknownError"));
            e.appendChild(b)
        }));
        c.appendChild(e);
        c = new CustomDialog(this, c, mxUtils.bind(this, function() {
            this.spinner.spin(document.body, mxResources.get("loading"));
            var a = 0,
                c;
            for (c in b) null == f[c] && (a++, mxUtils.bind(this, function(b) {
                this.remoteInvoke("getFileContent", [b.downloadUrl], null, mxUtils.bind(this, function(c) {
                    a--;
                    0 == a && this.spinner.stop();
                    try {
                        this.loadLibrary(new RemoteLibrary(this, c, b))
                    } catch (B) {
                        this.handleError(B, mxResources.get("errorLoadingFile"))
                    }
                }), mxUtils.bind(this, function() {
                    a--;
                    0 == a && this.spinner.stop();
                    this.handleError(null, mxResources.get("errorLoadingFile"))
                }))
            })(b[c]));
            for (c in f) b[c] || this.closeLibrary(new RemoteLibrary(this, null, f[c]));
            0 == a && this.spinner.stop()
        }), null, null, "https://desk.draw.io/support/solutions/articles/16000092763");
        this.showDialog(c.container, 340, 375, !0, !0, null, null, null, null, !0)
    };
    EditorUi.prototype.remoteInvokableFns = {
        getDiagramTextContent: {
            isAsync: !1
        },
        getLocalStorageFile: {
            isAsync: !1,
            allowedDomains: ["app.diagrams.net"]
        },
        getLocalStorageFileNames: {
            isAsync: !1,
            allowedDomains: ["app.diagrams.net"]
        },
        setMigratedFlag: {
            isAsync: !1,
            allowedDomains: ["app.diagrams.net"]
        }
    };
    EditorUi.prototype.remoteInvokeCallbacks = [];
    EditorUi.prototype.remoteInvokeQueue = [];
    EditorUi.prototype.handleRemoteInvokeReady = function(a) {
        this.remoteWin = a;
        for (var b = 0; b < this.remoteInvokeQueue.length; b++) a.postMessage(this.remoteInvokeQueue[b], "*");
        this.remoteInvokeQueue = []
    };
    EditorUi.prototype.handleRemoteInvokeResponse = function(a) {
        var b = a.msgMarkers,
            c = this.remoteInvokeCallbacks[b.callbackId];
        if (null == c) throw Error("No callback for " + (null != b ? b.callbackId : "null"));
        a.error ? c.error && c.error(a.error.errResp) : c.callback && c.callback.apply(this, a.resp);
        this.remoteInvokeCallbacks[b.callbackId] = null
    };
    EditorUi.prototype.remoteInvoke = function(a, b, c, e, f) {
        var d = !0,
            g = window.setTimeout(mxUtils.bind(this, function() {
                d = !1;
                f({
                    code: App.ERROR_TIMEOUT,
                    message: mxResources.get("timeout")
                })
            }), this.timeout),
            k = mxUtils.bind(this, function() {
                window.clearTimeout(g);
                d && e.apply(this, arguments)
            }),
            l = mxUtils.bind(this, function() {
                window.clearTimeout(g);
                d && f.apply(this, arguments)
            });
        c = c || {};
        c.callbackId = this.remoteInvokeCallbacks.length;
        this.remoteInvokeCallbacks.push({
            callback: k,
            error: l
        });
        a = JSON.stringify({
            event: "remoteInvoke",
            funtionName: a,
            functionArgs: b,
            msgMarkers: c
        });
        null != this.remoteWin ? this.remoteWin.postMessage(a, "*") : this.remoteInvokeQueue.push(a)
    };
    EditorUi.prototype.handleRemoteInvoke = function(a, b) {
        var c = mxUtils.bind(this, function(b, c) {
            var d = {
                event: "remoteInvokeResponse",
                msgMarkers: a.msgMarkers
            };
            null != c ? d.error = {
                errResp: c
            } : null != b && (d.resp = b);
            this.remoteWin.postMessage(JSON.stringify(d), "*")
        });
        try {
            var d = a.funtionName,
                e = this.remoteInvokableFns[d];
            if (null != e && "function" === typeof this[d]) {
                if (e.allowedDomains) {
                    for (var f = !1, g = 0; g < e.allowedDomains.length; g++)
                        if (b == "https://" + e.allowedDomains[g]) {
                            f = !0;
                            break
                        }
                    if (!f) {
                        c(null, "Invalid Call: " + d + " is not allowed.");
                        return
                    }
                }
                var k = a.functionArgs;
                Array.isArray(k) || (k = []);
                if (e.isAsync) k.push(function() {
                    c(Array.prototype.slice.apply(arguments))
                }), k.push(function(a) {
                    c(null, a || "Unkown Error")
                }), this[d].apply(this, k);
                else {
                    var l = this[d].apply(this, k);
                    c([l])
                }
            } else c(null, "Invalid Call: " + d + " is not found.")
        } catch (x) {
            c(null, "Invalid Call: An error occured, " + x.message)
        }
    };
    EditorUi.prototype.openDatabase = function(a, b) {
        if (null == this.database) {
            var c = window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB;
            if (null != c) try {
                var d = c.open("database", 2);
                d.onupgradeneeded = function(a) {
                    try {
                        var c = d.result;
                        1 > a.oldVersion && c.createObjectStore("objects", {
                            keyPath: "key"
                        });
                        2 > a.oldVersion && (c.createObjectStore("files", {
                            keyPath: "title"
                        }), c.createObjectStore("filesInfo", {
                            keyPath: "title"
                        }), EditorUi.migrateStorageFiles = isLocalStorage)
                    } catch (u) {
                        null != b && b(u)
                    }
                };
                d.onsuccess = mxUtils.bind(this, function(b) {
                    var c = d.result;
                    this.database = c;
                    EditorUi.migrateStorageFiles && (StorageFile.migrate(c), EditorUi.migrateStorageFiles = !1);
                    "app.diagrams.net" != location.host || this.drawioMigrationStarted || (this.drawioMigrationStarted = !0, this.getDatabaseItem(".drawioMigrated3", mxUtils.bind(this, function(a) {
                        if (!a || "1" == urlParams.forceMigration) {
                            var b = document.createElement("iframe");
                            b.style.display = "none";
                            b.setAttribute("src", "https://www.draw.io?embed=1&proto=json&forceMigration=" + urlParams.forceMigration);
                            document.body.appendChild(b);
                            var c = !0,
                                d = !1,
                                e, f = 0,
                                g = mxUtils.bind(this, function() {
                                    d = !0;
                                    this.setDatabaseItem(".drawioMigrated3", !0);
                                    b.contentWindow.postMessage(JSON.stringify({
                                        action: "remoteInvoke",
                                        funtionName: "setMigratedFlag"
                                    }), "*")
                                }),
                                k = mxUtils.bind(this, function() {
                                    f++;
                                    l()
                                }),
                                l = mxUtils.bind(this, function() {
                                    try {
                                        if (f >= e.length) g();
                                        else {
                                            var a = e[f];
                                            StorageFile.getFileContent(this, a, mxUtils.bind(this, function(c) {
                                                null == c || ".scratchpad" == a && c == this.emptyLibraryXml ? b.contentWindow.postMessage(JSON.stringify({
                                                    action: "remoteInvoke",
                                                    funtionName: "getLocalStorageFile",
                                                    functionArgs: [a]
                                                }), "*") : k()
                                            }), k)
                                        }
                                    } catch (F) {
                                        console.log(F)
                                    }
                                }),
                                m = mxUtils.bind(this, function(a) {
                                    try {
                                        this.setDatabaseItem(null, [{
                                            title: a.title,
                                            size: a.data.length,
                                            lastModified: Date.now(),
                                            type: a.isLib ? "L" : "F"
                                        }, {
                                            title: a.title,
                                            data: a.data
                                        }], k, k, ["filesInfo", "files"])
                                    } catch (F) {
                                        console.log(F)
                                    }
                                });
                            a = mxUtils.bind(this, function(a) {
                                try {
                                    if (a.source == b.contentWindow) {
                                        var f = {};
                                        try {
                                            f = JSON.parse(a.data)
                                        } catch (H) {}
                                        "init" == f.event ? (b.contentWindow.postMessage(JSON.stringify({
                                            action: "remoteInvokeReady"
                                        }), "*"), b.contentWindow.postMessage(JSON.stringify({
                                            action: "remoteInvoke",
                                            funtionName: "getLocalStorageFileNames"
                                        }), "*")) : "remoteInvokeResponse" != f.event || d || (c ? null != f.resp && 0 < f.resp.length && null != f.resp[0] ? (e = f.resp[0], c = !1, l()) : g() : null != f.resp && 0 < f.resp.length && null != f.resp[0] ? m(f.resp[0]) : k())
                                    }
                                } catch (H) {
                                    console.log(H)
                                }
                            });
                            window.addEventListener("message", a)
                        }
                    })));
                    a(c);
                    c.onversionchange = function() {
                        c.close()
                    }
                });
                d.onerror = b;
                d.onblocked = function() {}
            } catch (p) {
                null != b && b(p)
            } else null != b && b()
        } else a(this.database)
    };
    EditorUi.prototype.setDatabaseItem = function(a, b, c, e, f) {
        this.openDatabase(mxUtils.bind(this, function(d) {
            try {
                f = f || "objects";
                Array.isArray(f) || (f = [f], a = [a], b = [b]);
                var g = d.transaction(f, "readwrite");
                g.oncomplete = c;
                g.onerror = e;
                for (d = 0; d < f.length; d++) g.objectStore(f[d]).put(null != a && null != a[d] ? {
                    key: a[d],
                    data: b[d]
                } : b[d])
            } catch (q) {
                null != e && e(q)
            }
        }), e)
    };
    EditorUi.prototype.removeDatabaseItem = function(a, b, c, e) {
        this.openDatabase(mxUtils.bind(this, function(d) {
            e = e || "objects";
            Array.isArray(e) || (e = [e], a = [a]);
            d = d.transaction(e, "readwrite");
            d.oncomplete = b;
            d.onerror = c;
            for (var f = 0; f < e.length; f++) d.objectStore(e[f])["delete"](a[f])
        }), c)
    };
    EditorUi.prototype.getDatabaseItem = function(a, b, c, e) {
        this.openDatabase(mxUtils.bind(this, function(d) {
            try {
                e = e || "objects";
                var f = d.transaction([e], "readonly").objectStore(e).get(a);
                f.onsuccess = function() {
                    b(f.result)
                };
                f.onerror = c
            } catch (u) {
                null != c && c(u)
            }
        }), c)
    };
    EditorUi.prototype.getDatabaseItems = function(a, b, c) {
        this.openDatabase(mxUtils.bind(this, function(d) {
            try {
                c = c || "objects";
                var e = d.transaction([c], "readonly").objectStore(c).openCursor(IDBKeyRange.lowerBound(0)),
                    f = [];
                e.onsuccess = function(b) {
                    null == b.target.result ? a(f) : (f.push(b.target.result.value), b.target.result["continue"]())
                };
                e.onerror = b
            } catch (u) {
                null != b && b(u)
            }
        }), b)
    };
    EditorUi.prototype.getDatabaseItemKeys = function(a, b, c) {
        this.openDatabase(mxUtils.bind(this, function(d) {
            try {
                c = c || "objects";
                var e = d.transaction([c], "readonly").objectStore(c).getAllKeys();
                e.onsuccess = function() {
                    a(e.result)
                };
                e.onerror = b
            } catch (t) {
                null != b && b(t)
            }
        }), b)
    };
    EditorUi.prototype.commentsSupported = function() {
        var a = this.getCurrentFile();
        return null != a ? a.commentsSupported() : !1
    };
    EditorUi.prototype.commentsRefreshNeeded = function() {
        var a = this.getCurrentFile();
        return null != a ? a.commentsRefreshNeeded() : !0
    };
    EditorUi.prototype.commentsSaveNeeded = function() {
        var a = this.getCurrentFile();
        return null != a ? a.commentsSaveNeeded() : !1
    };
    EditorUi.prototype.getComments = function(a, b) {
        var c = this.getCurrentFile();
        null != c ? c.getComments(a, b) : a([])
    };
    EditorUi.prototype.addComment = function(a, b, c) {
        var d = this.getCurrentFile();
        null != d ? d.addComment(a, b, c) : b(Date.now())
    };
    EditorUi.prototype.canReplyToReplies = function() {
        var a = this.getCurrentFile();
        return null != a ? a.canReplyToReplies() : !0
    };
    EditorUi.prototype.canComment = function() {
        var a = this.getCurrentFile();
        return null != a ? a.canComment() : !0
    };
    EditorUi.prototype.newComment = function(a, b) {
        var c = this.getCurrentFile();
        return null != c ? c.newComment(a, b) : new DrawioComment(this, null, a, Date.now(), Date.now(), !1, b)
    };
    EditorUi.prototype.isRevisionHistorySupported = function() {
        var a = this.getCurrentFile();
        return null != a && a.isRevisionHistorySupported()
    };
    EditorUi.prototype.getRevisions = function(a, b) {
        var c = this.getCurrentFile();
        null != c && c.getRevisions ? c.getRevisions(a, b) : b({
            message: mxResources.get("unknownError")
        })
    };
    EditorUi.prototype.isRevisionHistoryEnabled = function() {
        var a = this.getCurrentFile();
        return null != a && (a.constructor == DriveFile && a.isEditable() || a.constructor == DropboxFile)
    };
    EditorUi.prototype.getServiceName = function() {
        return "draw.io"
    };
    EditorUi.prototype.addRemoteServiceSecurityCheck = function(a) {
        a.setRequestHeader("Content-Language", "da, mi, en, de-DE")
    };
    EditorUi.prototype.loadUrl = function(a, b, c, e, f, k, l, q) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: loadUrl");
        return this.editor.loadUrl(a, b, c, e, f, k, l, q)
    };
    EditorUi.prototype.loadFonts = function(a) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: loadFonts");
        return this.editor.loadFonts(a)
    };
    EditorUi.prototype.createSvgDataUri = function(a) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: createSvgDataUri");
        return Editor.createSvgDataUri(a)
    };
    EditorUi.prototype.embedCssFonts = function(a, b) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: embedCssFonts");
        return this.editor.embedCssFonts(a, b)
    };
    EditorUi.prototype.embedExtFonts = function(a) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: embedExtFonts");
        return this.editor.embedExtFonts(a)
    };
    EditorUi.prototype.exportToCanvas = function(a, b, c, e, f, k, l, q, v, x, y, A, z, B, C, D) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: exportToCanvas");
        return this.editor.exportToCanvas(a, b, c, e, f, k, l, q, v, x, y, A, z, B, C, D)
    };
    EditorUi.prototype.createImageUrlConverter = function() {
        EditorUi.logEvent("SHOULD NOT BE CALLED: createImageUrlConverter");
        return this.editor.createImageUrlConverter()
    };
    EditorUi.prototype.convertImages = function(a, b, c, e) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: convertImages");
        return this.editor.convertImages(a, b, c, e)
    };
    EditorUi.prototype.convertImageToDataUri = function(a, b) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: convertImageToDataUri");
        return this.editor.convertImageToDataUri(a, b)
    };
    EditorUi.prototype.base64Encode = function(a) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: base64Encode");
        return Editor.base64Encode(a)
    };
    EditorUi.prototype.updateCRC = function(a, b, c, e) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: updateCRC");
        return Editor.updateCRC(a, b, c, e)
    };
    EditorUi.prototype.crc32 = function(a) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: crc32");
        return Editor.crc32(a)
    };
    EditorUi.prototype.writeGraphModelToPng = function(a, b, c, e, f) {
        EditorUi.logEvent("SHOULD NOT BE CALLED: writeGraphModelToPng");
        return Editor.writeGraphModelToPng(a, b, c, e, f)
    };
    EditorUi.prototype.getLocalStorageFileNames = function() {
        if ("1" == localStorage.getItem(".localStorageMigrated") && "1" != urlParams.forceMigration) return null;
        for (var a = [], b = 0; b < localStorage.length; b++) {
            var c = localStorage.key(b),
                e = localStorage.getItem(c);
            if (0 < c.length && (".scratchpad" == c || "." != c.charAt(0)) && 0 < e.length) {
                var f = "<mxfile " === e.substring(0, 8) || "<?xml" === e.substring(0, 5) || "<!--[if IE]>" === e.substring(0, 12),
                    e = "<mxlibrary>" === e.substring(0, 11);
                (f || e) && a.push(c)
            }
        }
        return a
    };
    EditorUi.prototype.getLocalStorageFile = function(a) {
        if ("1" == localStorage.getItem(".localStorageMigrated") && "1" != urlParams.forceMigration) return null;
        var b = localStorage.getItem(a);
        return {
            title: a,
            data: b,
            isLib: "<mxlibrary>" === b.substring(0, 11)
        }
    };
    EditorUi.prototype.setMigratedFlag = function() {
        localStorage.setItem(".localStorageMigrated", "1")
    }
})();

(function() {
    var a = mxPopupMenu.prototype.showMenu;
    mxPopupMenu.prototype.showMenu = function() {
        a.apply(this, arguments);
        this.div.style.overflowY = "auto";
        this.div.style.overflowX = "hidden";
        this.div.style.maxHeight = Math.max(document.body.clientHeight, document.documentElement.clientHeight) - 10 + "px"
    };
    Menus.prototype.createHelpLink = function(a) {
        var b = document.createElement("span");
        b.setAttribute("title", mxResources.get("help"));
        b.style.cssText = "color:blue;text-decoration:underline;margin-left:8px;cursor:help;";
        var c = document.createElement("img");
        mxUtils.setOpacity(c, 50);
        c.style.height = "16px";
        c.style.width = "16px";
        c.setAttribute("border", "0");
        c.setAttribute("valign", "bottom");
        c.setAttribute("src", Editor.helpImage);
        b.appendChild(c);
        mxEvent.addGestureListeners(b, mxUtils.bind(this, function(b) {
            null != this.editorUi.menubar && this.editorUi.menubar.hideMenu();
            this.editorUi.openLink(a);
            mxEvent.consume(b)
        }));
        return b
    };
    Menus.prototype.addLinkToItem = function(a, b) {
        null != a && a.firstChild.nextSibling.appendChild(this.createHelpLink(b))
    };
    var e = Menus.prototype.init;
    Menus.prototype.init = function() {
        function a(a, b, c) {
            this.ui = a;
            this.previousExtFonts = this.extFonts = b;
            this.prevCustomFonts = this.customFonts = c
        }
        e.apply(this, arguments);
        var b = this.editorUi,
            k = b.editor.graph,
            f = mxUtils.bind(k, k.isEnabled),
            l = ("1" != urlParams.embed && "0" != urlParams.gapi || "1" == urlParams.embed && "1" == urlParams.gapi) && mxClient.IS_SVG && isLocalStorage && (null == document.documentMode || 10 <= document.documentMode),
            d = ("1" != urlParams.embed && "0" != urlParams.db || "1" == urlParams.embed && "1" == urlParams.db) && mxClient.IS_SVG && (null == document.documentMode || 9 < document.documentMode),
            g = ("www.draw.io" == window.location.hostname || "test.draw.io" == window.location.hostname || "drive.draw.io" == window.location.hostname || "app.diagrams.net" == window.location.hostname) && ("1" != urlParams.embed && "0" != urlParams.od || "1" == urlParams.embed && "1" == urlParams.od) && !mxClient.IS_IOS && (0 > navigator.userAgent.indexOf("MSIE") || 10 <= document.documentMode),
            m = ("1" != urlParams.embed && "0" != urlParams.tr || "1" == urlParams.embed && "1" == urlParams.tr) && mxClient.IS_SVG && (null == document.documentMode || 9 < document.documentMode);
        mxClient.IS_SVG || b.isOffline() || ((new Image).src = IMAGE_PATH + "/help.png");
        b.actions.addAction("new...", function() {
            var a = b.isOffline(),
                c = new NewDialog(b, a, !(b.mode == App.MODE_DEVICE && "chooseFileSystemEntries" in window));
            b.showDialog(c.container, a ? 350 : 620, a ? 70 : 440, !0, !0, function(a) {
                a && null == b.getCurrentFile() && b.showSplash()
            });
            c.init()
        });
        b.actions.put("insertTemplate", new Action(mxResources.get("template") + "...", function() {
            var a = new NewDialog(b, null, !1, function(a) {
                b.hideDialog();
                if (null != a) {
                    var c = b.editor.graph.getFreeInsertPoint();
                    k.setSelectionCells(b.importXml(a, Math.max(c.x, 20), Math.max(c.y, 20), !0));
                    k.scrollCellToVisible(k.getSelectionCell())
                }
            }, null, null, null, null, null, null, null, null, null, null, !1, mxResources.get("insert"));
            b.showDialog(a.container, 620, 440, !0, !0)
        })).isEnabled = f;
        var n = b.actions.addAction("points", function() {
            b.editor.graph.view.setUnit(mxConstants.POINTS)
        });
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return b.editor.graph.view.unit == mxConstants.POINTS
        });
        n = b.actions.addAction("inches", function() {
            b.editor.graph.view.setUnit(mxConstants.INCHES)
        });
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return b.editor.graph.view.unit == mxConstants.INCHES
        });
        n = b.actions.addAction("millimeters", function() {
            b.editor.graph.view.setUnit(mxConstants.MILLIMETERS)
        });
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return b.editor.graph.view.unit == mxConstants.MILLIMETERS
        });
        this.put("units", new Menu(mxUtils.bind(this, function(a, b) {
            this.addMenuItems(a, ["points", "millimeters"], b)
        })));
        n = b.actions.addAction("ruler", function() {
            mxSettings.setRulerOn(!mxSettings.isRulerOn());
            mxSettings.save();
            null != b.ruler ? (b.ruler.destroy(), b.ruler = null) : b.ruler = new mxDualRuler(b, b.editor.graph.view.unit);
            b.refresh()
        });
        n.setEnabled(b.canvasSupported && 9 != document.documentMode);
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return null != b.ruler
        });
        b.actions.addAction("properties...", function() {
            var a = new FilePropertiesDialog(b);
            b.showDialog(a.container, 320, 120, !0, !0);
            a.init()
        }).isEnabled = f;
        window.mxFreehand && (b.actions.put("insertFreehand", new Action(mxResources.get("freehand") + "...", function(a) {
            k.isEnabled() && (null == this.freehandWindow && (this.freehandWindow = new FreehandWindow(b, document.body.offsetWidth - 420, 102, 176, 104)), k.freehand.isDrawing() ? k.freehand.stopDrawing() : k.freehand.startDrawing(), this.freehandWindow.window.setVisible(k.freehand.isDrawing()))
        })).isEnabled = function() {
            return f() && mxClient.IS_SVG
        });
        // b.actions.put("exportXml", new Action(mxResources.get("formatXml") + "...", function () {
        //   var a = document.createElement("div");
        //   a.style.whiteSpace = "nowrap";
        //   var c = null == b.pages || 1 >= b.pages.length,
        //     d = document.createElement("h3");
        //   mxUtils.write(d, mxResources.get("formatXml"));
        //   d.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:4px";
        //   a.appendChild(d);
        //   var e = b.addCheckbox(a, mxResources.get("selectionOnly"), !1, k.isSelectionEmpty()),
        //     f = b.addCheckbox(a, mxResources.get("compressed"), !0),
        //     g = b.addCheckbox(a, mxResources.get("allPages"), !c, c);
        //   g.style.marginBottom = "16px";
        //   mxEvent.addListener(e, "change", function () {
        //     e.checked ? g.setAttribute("disabled", "disabled") : g.removeAttribute("disabled")
        //   });
        //   a = new CustomDialog(b, a, mxUtils.bind(this, function () {
        //     b.downloadFile("xml", !f.checked, null, !e.checked, c || !g.checked)
        //   }), null, mxResources.get("export"));
        //   b.showDialog(a.container, 300, 180, !0, !0)
        // }));
        b.actions.put("exportUrl", new Action(mxResources.get("url") + "...", function() {
            b.showPublishLinkDialog(mxResources.get("url"), !0, null, null, function(a, c, d, e, f, g) {
                a = new EmbedDialog(b, b.createLink(a, c, d, e, f, g, null, !0));
                b.showDialog(a.container, 440, 240, !0, !0);
                a.init()
            })
        }));
        b.actions.put("exportHtml", new Action(mxResources.get("formatHtmlEmbedded") + "...", function() {
            b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(a) {
                b.spinner.stop();
                b.showHtmlDialog(mxResources.get("export"), null, a, function(a, c, d, e, f, g, k, l, m, n) {
                    b.createHtml(a, c, d, e, f, g, k, l, m, n, mxUtils.bind(this, function(a, c) {
                        var d = b.getBaseFilename(k),
                            e = '<!--[if IE]><meta http-equiv="X-UA-Compatible" content="IE=5,IE=9" ><![endif]-->\n<!DOCTYPE html>\n<html>\n<head>\n<title>' + mxUtils.htmlEntities(d) + '</title>\n<meta charset="utf-8"/>\n</head>\n<body>' + a + "\n" + c + "\n</body>\n</html>";
                        b.saveData(d + ".html", "html", e, "text/html")
                    }))
                })
            })
        }));
        b.actions.put("exportPdf", new Action(mxResources.get("formatPdf") + "...", function() {
            if (EditorUi.isElectronApp || !b.isOffline() && !b.printPdfExport) {
                var a = null == b.pages || 1 >= b.pages.length,
                    c = document.createElement("div");
                c.style.whiteSpace = "nowrap";
                var d = document.createElement("h3");
                mxUtils.write(d, mxResources.get("formatPdf"));
                d.style.cssText = "width:100%;text-align:center;margin-top:0px;margin-bottom:4px";
                c.appendChild(d);
                var e = function() {
                        f != this && this.checked ? (m.removeAttribute("disabled"), m.checked = !k.pageVisible) : (m.setAttribute("disabled", "disabled"), m.checked = !1)
                    },
                    d = 180;
                if (b.pdfPageExport && !a) {
                    var f = b.addRadiobox(c, "pages", mxResources.get("allPages"), !0),
                        g = b.addRadiobox(c, "pages", mxResources.get("currentPage"), !1),
                        l = b.addRadiobox(c, "pages", mxResources.get("selectionOnly"), !1, k.isSelectionEmpty()),
                        m = b.addCheckbox(c, mxResources.get("crop"), !1, !0),
                        n = b.addCheckbox(c, mxResources.get("grid"), !1, !1);
                    mxEvent.addListener(f, "change", e);
                    mxEvent.addListener(g, "change", e);
                    mxEvent.addListener(l, "change", e);
                    d += 60
                } else l = b.addCheckbox(c, mxResources.get("selectionOnly"), !1, k.isSelectionEmpty()), m = b.addCheckbox(c, mxResources.get("crop"), !k.pageVisible || !b.pdfPageExport, !b.pdfPageExport), n = b.addCheckbox(c, mxResources.get("grid"), !1, !1), b.pdfPageExport || mxEvent.addListener(l, "change", e);
                var p = null,
                    v = null;
                mxClient.IS_CHROMEAPP || EditorUi.isElectronApp || "draw.io" != b.getServiceName() || (v = b.addCheckbox(c, mxResources.get("includeCopyOfMyDiagram"), !0), p = b.addCheckbox(c, mxResources.get("transparentBackground"), !1), d += 60);
                c = new CustomDialog(b, c, mxUtils.bind(this, function() {
                    b.downloadFile("pdf", null, null, !l.checked, a ? !0 : !f.checked, !m.checked, null != p && p.checked, null, null, n.checked, null != v && v.checked)
                }), null, mxResources.get("export"));
                b.showDialog(c.container, 300, d, !0, !0)
            } else b.showDialog((new PrintDialog(b, mxResources.get("formatPdf"))).container, 360, null != b.pages && 1 < b.pages.length && (b.editor.editable || "1" != urlParams["hide-pages"]) ? 450 : 370, !0, !0)
        }));
        b.actions.addAction("open...", function() {
            b.pickFile()
        });
        b.actions.addAction("close", function() {
            function a() {
                null != c && c.removeDraft();
                b.fileLoaded(null)
            }
            var c = b.getCurrentFile();
            null != c && c.isModified() ? b.confirm(mxResources.get("allChangesLost"), null, a, mxResources.get("cancel"), mxResources.get("discardChanges")) : a()
        });
        b.actions.addAction("editShape...", mxUtils.bind(this, function() {
            k.getSelectionCells();
            if (1 == k.getSelectionCount()) {
                var a = k.getSelectionCell(),
                    c = k.view.getState(a);
                null != c && null != c.shape && null != c.shape.stencil && (a = new EditShapeDialog(b, a, mxResources.get("editShape") + ":", 630, 400), b.showDialog(a.container, 640, 480, !0, !1), a.init())
            }
        }));
        b.actions.addAction("revisionHistory...", function() {
            b.isRevisionHistorySupported() ? b.spinner.spin(document.body, mxResources.get("loading")) && b.getRevisions(mxUtils.bind(this, function(a, c) {
                b.spinner.stop();
                var d = new RevisionDialog(b, a, c);
                b.showDialog(d.container, 640, 480, !0, !0);
                d.init()
            }), mxUtils.bind(this, function(a) {
                b.handleError(a)
            })) : b.showError(mxResources.get("error"), mxResources.get("notAvailable"), mxResources.get("ok"))
        });
        b.actions.addAction("createRevision", function() {
            b.actions.get("save").funct()
        }, null, null, Editor.ctrlKey + "+S");
        n = b.actions.addAction("synchronize", function() {
            b.synchronizeCurrentFile("none" == DrawioFile.SYNC)
        }, null, null, "Alt+Shift+S");
        "none" == DrawioFile.SYNC && (n.label = mxResources.get("refresh"));
        b.actions.addAction("upload...", function() {
            var a = b.getCurrentFile();
            null != a && (window.drawdata = b.getFileData(), a = null != a.getTitle() ? a.getTitle() : b.defaultFilename, b.openLink(window.location.protocol + "//" + window.location.host + "/?create=drawdata&" + (b.mode == App.MODE_DROPBOX ? "mode=dropbox&" : "") + "title=" + encodeURIComponent(a), null, !0))
        });
        "undefined" !== typeof MathJax && (n = b.actions.addAction("mathematicalTypesetting", function() {
            var a = new ChangePageSetup(b);
            a.ignoreColor = !0;
            a.ignoreImage = !0;
            a.mathEnabled = !b.isMathEnabled();
            k.model.execute(a)
        }), n.setToggleAction(!0), n.setSelectedCallback(function() {
            return b.isMathEnabled()
        }), n.isEnabled = f);
        isLocalStorage && (n = b.actions.addAction("showStartScreen", function() {
            mxSettings.setShowStartScreen(!mxSettings.getShowStartScreen());
            mxSettings.save()
        }), n.setToggleAction(!0), n.setSelectedCallback(function() {
            return mxSettings.getShowStartScreen()
        }));
        var p = b.actions.addAction("autosave", function() {
            b.editor.setAutosave(!b.editor.autosave)
        });
        p.setToggleAction(!0);
        p.setSelectedCallback(function() {
            return p.isEnabled() && b.editor.autosave
        });
        b.actions.addAction("editGeometry...", function() {
            for (var a = k.getSelectionCells(), c = [], d = 0; d < a.length; d++) k.getModel().isVertex(a[d]) && c.push(a[d]);
            0 < c.length && (a = new EditGeometryDialog(b, c), b.showDialog(a.container, 200, 250, !0, !0), a.init())
        }, null, null, Editor.ctrlKey + "+Shift+M");
        var t = null;
        b.actions.addAction("setProblemSolvingLayerStyle", function() {
            a = { pd3layer: "topic", fillColor: "#ffe6cc", strokeColor: "#d79b00", fontColor: "#000000" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.addAction("setInformationLayerStyle", function() {
            a = { pd3layer: "info", fillColor: "#dae8fc", strokeColor: "#6c8ebf", fontColor: "#000000" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.addAction("setPhysicalLayerStyle", function() {
            a = { pd3layer: "phys", fillColor: "#d5e8d4", strokeColor: "#82b366", fontColor: "#000000" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.addAction("setDPBoxStyle", function() {
            a = { pd3layer: "topic", fillColor: "#D1BC35", strokeColor: "#d79b00", fontColor: "#ffffff", pd3action: "ECDP" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        // b.actions.addAction("setDataCollectionBoxStyle", function () {
        //   a = {pd3layer: "topic", fillColor: "#60a917", strokeColor:"#d79b00", fontColor: "#ffffff", pd3action: "ECCD"}
        //   k.isEnabled() && !k.isSelectionEmpty()  && k.setPD3Style(a, k.getSelectionCells())
        // }, null, null, null);
        b.actions.addAction("setCAIBoxStyle", function() {
            a = { pd3layer: "topic", fillColor: "#3E54E6", strokeColor: "#d79b00", fontColor: "#ffffff", pd3action: "ECCAI" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        // b.actions.addAction("setEvaluationBoxStyle", function () {
        //   a = {pd3layer: "topic", fillColor: "#6a00ff", strokeColor:"#d79b00", fontColor: "#ffffff", pd3action: "ECEV"}
        //   k.isEnabled() && !k.isSelectionEmpty()  && k.setPD3Style(a, k.getSelectionCells())
        // }, null, null, null);
        b.actions.addAction("setGHBoxStyle", function() {
            a = { pd3layer: "topic", fillColor: "#C93AC9", strokeColor: "#d79b00", fontColor: "#ffffff", pd3action: "ECGH" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.addAction("setESIBoxStyle", function() {
            a = { pd3layer: "topic", fillColor: "#8F4132", strokeColor: "#d79b00", fontColor: "#ffffff", pd3action: "ECESI" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.addAction("setEXBoxStyle", function() {
            a = { pd3layer: "topic", fillColor: "#2EBAC9", strokeColor: "#d79b00", fontColor: "#ffffff", pd3action: "ECEX" }
            k.isEnabled() && !k.isSelectionEmpty() && k.setPD3Style(a, k.getSelectionCells())
        }, null, null, null);
        b.actions.put("pageBackgroundImage", new Action(mxResources.get("backgroundImage") + "...", function() {
            if (!b.isOffline()) {
                var a = new BackgroundImageDialog(b, function(a) {
                    b.setBackgroundImage(a)
                });
                b.showDialog(a.container, 320, 170, !0, !0);
                a.init()
            }
        }));
        b.actions.put("exportSvg", new Action(mxResources.get("formatSvg") + "...", function() {
            b.showExportDialog(mxResources.get("formatSvg"), !0, mxResources.get("export"), "https://desk.draw.io/support/solutions/articles/16000067785", mxUtils.bind(this, function(a, c, d, e, f, g, k, l, m, n, p, v) {
                a = parseInt(a);
                !isNaN(a) && 0 < a && b.exportSvg(a / 100, c, d, e, f, g, k, !l, m, n, v)
            }), !0, null, "svg")
        }));
        b.actions.put("exportPng", new Action(mxResources.get("formatPng") + "...", function() {
            b.isExportToCanvas() ? b.showExportDialog(mxResources.get("image"), !1, mxResources.get("export"), "https://desk.draw.io/support/solutions/articles/16000067785", mxUtils.bind(this, function(a, c, d, e, f, g, k, l, m, n, p, v) {
                a = parseInt(a);
                !isNaN(a) && 0 < a && b.exportImage(a / 100, c, d, e, f, k, !l, m, null, p, null, v)
            }), !0, !0, "png") : b.isOffline() || mxClient.IS_IOS && navigator.standalone || b.showRemoteExportDialog(mxResources.get("export"), null, mxUtils.bind(this, function(a, c, d, e, f) {
                b.downloadFile(c ? "xmlpng" : "png", null, null, a, null, null, d, e, f)
            }), !1, !0)
        }));
        b.actions.put("exportJpg", new Action(mxResources.get("formatJpg") + "...", function() {
            b.isExportToCanvas() ? b.showExportDialog(mxResources.get("image"), !1, mxResources.get("export"), "https://desk.draw.io/support/solutions/articles/16000067785", mxUtils.bind(this, function(a, c, d, e, f, g, k, l, m, n, p, v) {
                a = parseInt(a);
                !isNaN(a) && 0 < a && b.exportImage(a / 100, !1, d, e, !1, k, !l, !1, "jpeg", p, null, v)
            }), !0, !1, "jpeg") : b.isOffline() || mxClient.IS_IOS && navigator.standalone || b.showRemoteExportDialog(mxResources.get("export"), null, mxUtils.bind(this, function(a, c, d, e, f) {
                b.downloadFile("jpeg", null, null, a, null, null, null, e, f)
            }), !0, !0)
        }));
        n = b.actions.put("shadowVisible", new Action(mxResources.get("shadow"), function() {
            k.setShadowVisible(!k.shadowVisible)
        }));
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return k.shadowVisible
        });
        b.actions.put("about", new Action(mxResources.get("about") + " " + EditorUi.VERSION + "...", function() {
            b.isOffline() || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp ? b.alert(b.editor.appName + " " + EditorUi.VERSION) : b.openLink("https://www.diagrams.net/")
        }));
        b.actions.addAction("support...", function() {
            b.openLink("https://github.com/jgraph/drawio/wiki/Getting-Support")
        });
        b.actions.addAction("exportOptionsDisabled...", function() {
            b.handleError({
                message: mxResources.get("exportOptionsDisabledDetails")
            }, mxResources.get("exportOptionsDisabled"))
        });
        b.actions.addAction("keyboardShortcuts...", function() {
            !mxClient.IS_SVG || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp ? b.openLink("https://viewer.diagrams.net/#Uhttps%3A%2F%2Fviewer.diagrams.net%2Fshortcuts.svg") : b.openLink("shortcuts.svg")
        });
        b.actions.addAction("feedback...", function() {
            var a = new FeedbackDialog(b);
            b.showDialog(a.container, 610, 360, !0, !1);
            a.init()
        });
        b.actions.addAction("quickStart...", function() {
            b.openLink("https://www.youtube.com/watch?v=Z0D96ZikMkc")
        });
        n = b.actions.addAction("tags...", mxUtils.bind(this, function() {
            null == this.tagsWindow ? (this.tagsWindow = new TagsWindow(b, document.body.offsetWidth - 380, 230, 300, 120), this.tagsWindow.window.addListener("show", function() {
                b.fireEvent(new mxEventObject("tags"))
            }), this.tagsWindow.window.addListener("hide", function() {
                b.fireEvent(new mxEventObject("tags"))
            }), this.tagsWindow.window.setVisible(!0), b.fireEvent(new mxEventObject("tags"))) : this.tagsWindow.window.setVisible(!this.tagsWindow.window.isVisible())
        }));
        n.setToggleAction(!0);
        n.setSelectedCallback(mxUtils.bind(this, function() {
            return null != this.tagsWindow && this.tagsWindow.window.isVisible()
        }));
        n = b.actions.addAction("find...", mxUtils.bind(this, function() {
            null == this.findWindow ? (this.findWindow = new FindWindow(b, document.body.offsetWidth - 300, 110, 240, 155), this.findWindow.window.addListener("show", function() {
                b.fireEvent(new mxEventObject("find"))
            }), this.findWindow.window.addListener("hide", function() {
                b.fireEvent(new mxEventObject("find"))
            }), this.findWindow.window.setVisible(!0), b.fireEvent(new mxEventObject("find"))) : this.findWindow.window.setVisible(!this.findWindow.window.isVisible())
        }));
        n.setToggleAction(!0);
        n.setSelectedCallback(mxUtils.bind(this, function() {
            return null != this.findWindow && this.findWindow.window.isVisible()
        }));
        b.actions.put("exportVsdx", new Action(mxResources.get("formatVsdx") + " (beta)...", function() {
            b.exportVisio()
        }));
        isLocalStorage && null != localStorage && "1" != urlParams.embed && b.actions.addAction("configuration...", function() {
            var a = localStorage.getItem(".configuration"),
                a = new TextareaDialog(b, mxResources.get("configuration") + ":", null != a ? JSON.stringify(JSON.parse(a), null, 2) : "", function(a) {
                    if (null != a) try {
                        if (0 < a.length) {
                            var c = JSON.parse(a);
                            localStorage.setItem(".configuration", JSON.stringify(c))
                        } else localStorage.removeItem(".configuration");
                        b.hideDialog();
                        b.alert(mxResources.get("restartForChangeRequired"))
                    } catch (D) {
                        b.handleError(D)
                    }
                }, null, null, null, null, null, !0, null, null, "https://desk.draw.io/support/solutions/articles/16000058316", EditorUi.isElectronApp ? null : [
                    [mxResources.get("reset"),
                        function(a, c) {
                            b.confirm(mxResources.get("areYouSure"), function() {
                                try {
                                    localStorage.removeItem(".configuration"), localStorage.removeItem(".drawio-config"), localStorage.removeItem(".mode"), b.hideDialog(), b.alert(mxResources.get("restartForChangeRequired"))
                                } catch (D) {
                                    b.handleError(D)
                                }
                            })
                        }
                    ],
                    [mxResources.get("link"), function(a, c) {
                        if (0 < c.value.length) try {
                            var d = JSON.parse(c.value),
                                e = window.location.protocol + "//" + window.location.host + "/" + b.getSearch() + "#_CONFIG_" + Graph.compress(JSON.stringify(d)),
                                f = new EmbedDialog(b, e);
                            b.showDialog(f.container, 440, 240, !0);
                            f.init()
                        } catch (H) {
                            b.handleError(H)
                        } else b.handleError({
                            message: mxResources.get("invalidInput")
                        })
                    }]
                ]);
            a.textarea.style.width = "600px";
            a.textarea.style.height = "380px";
            b.showDialog(a.container, 620, 460, !0, !1);
            a.init()
        });
        if (mxClient.IS_CHROMEAPP || isLocalStorage) {
            this.put("language", new Menu(mxUtils.bind(this, function(a, c) {
                var d = mxUtils.bind(this, function(d) {
                    var e = "" == d ? mxResources.get("automatic") : mxLanguageMap[d],
                        f = null;
                    "" != e && (f = a.addItem(e, null, mxUtils.bind(this, function() {
                        mxSettings.setLanguage(d);
                        mxSettings.save();
                        mxClient.language = d;
                        mxResources.loadDefaultBundle = !1;
                        mxResources.add(RESOURCE_BASE);
                        b.alert(mxResources.get("restartForChangeRequired"))
                    }), c), (d == mxLanguage || "" == d && null == mxLanguage) && a.addCheckmark(f, Editor.checkmarkImage));
                    return f
                });
                d("");
                a.addSeparator(c);
                for (var e in mxLanguageMap) d(e)
            })));
            var u = Menus.prototype.createMenubar;
            Menus.prototype.createMenubar = function(a) {
                var b = u.apply(this, arguments);
                if (null != b) {
                    var c = this.get("language");
                    if (null != c) {
                        c = b.addMenu("", c.funct);
                        c.setAttribute("title", mxResources.get("language"));
                        c.style.width = "16px";
                        c.style.paddingTop = "2px";
                        c.style.paddingLeft = "4px";
                        c.style.zIndex = "1";
                        c.style.position = "absolute";
                        c.style.display = "block";
                        c.style.cursor = "pointer";
                        c.style.right = "17px";
                        "atlas" == uiTheme ? (c.style.top = "6px", c.style.right = "15px") : c.style.top = "min" == uiTheme ? "2px" : "0px";
                        if (mxClient.IS_VML) c.innerHTML = '<div class="geIcon geSprite geSprite-globe"/>';
                        else {
                            var d = document.createElement("div");
                            d.style.backgroundImage = "url(" + Editor.globeImage + ")";
                            d.style.backgroundPosition = "center center";
                            d.style.backgroundRepeat = "no-repeat";
                            d.style.backgroundSize = "19px 19px";
                            d.style.position = "absolute";
                            d.style.height = "19px";
                            d.style.width = "19px";
                            d.style.marginTop = "2px";
                            d.style.zIndex = "1";
                            c.appendChild(d);
                            mxUtils.setOpacity(c, 40);
                            if ("atlas" == uiTheme || "dark" == uiTheme) c.style.opacity = "0.85", c.style.filter = "invert(100%)"
                        }
                        document.body.appendChild(c)
                    }
                }
                return b
            }
        }
        b.customLayoutConfig = [{
            layout: "mxHierarchicalLayout",
            config: {
                orientation: "west",
                intraCellSpacing: 30,
                interRankCellSpacing: 100,
                interHierarchySpacing: 60,
                parallelEdgeSpacing: 10
            }
        }];
        b.actions.addAction("runLayout", function() {
            var a = new TextareaDialog(b, "Run Layouts:", JSON.stringify(b.customLayoutConfig, null, 2), function(a) {
                if (0 < a.length) try {
                    var c = JSON.parse(a);
                    b.executeLayoutList(c);
                    b.customLayoutConfig = c
                } catch (D) {
                    b.handleError(D), null != window.console && console.error(D)
                }
            });
            a.textarea.style.width = "600px";
            a.textarea.style.height = "380px";
            b.showDialog(a.container, 620, 460, !0, !0);
            a.init()
        });
        var n = this.get("layout"),
            q = n.funct;
        n.funct = function(a, c) {
            q.apply(this, arguments);
            a.addSeparator(c);
            a.addItem(mxResources.get("orgChart") + "...", null, function() {
                var a = null,
                    c = 20,
                    d = 20,
                    e = !0,
                    f = function() {
                        b.loadingOrgChart = !1;
                        b.spinner.stop();
                        if ("undefined" !== typeof mxOrgChartLayout && null != a && e) {
                            var f = b.editor.graph,
                                g = new mxOrgChartLayout(f, a, c, d),
                                k = f.getDefaultParent();
                            1 < f.model.getChildCount(f.getSelectionCell()) && (k = f.getSelectionCell());
                            g.execute(k);
                            e = !1
                        }
                    },
                    g = document.createElement("div"),
                    k = document.createElement("div");
                k.style.marginTop = "6px";
                k.style.display = "inline-block";
                k.style.width = "140px";
                mxUtils.write(k, mxResources.get("orgChartType") + ": ");
                g.appendChild(k);
                var l = document.createElement("select");
                l.style.width = "200px";
                l.style.boxSizing = "border-box";
                for (var k = [mxResources.get("linear"), mxResources.get("hanger2"), mxResources.get("hanger4"), mxResources.get("fishbone1"), mxResources.get("fishbone2"), mxResources.get("1ColumnLeft"), mxResources.get("1ColumnRight"), mxResources.get("smart")], m = 0; m < k.length; m++) {
                    var n = document.createElement("option");
                    mxUtils.write(n, k[m]);
                    n.value = m;
                    2 == m && n.setAttribute("selected", "selected");
                    l.appendChild(n)
                }
                mxEvent.addListener(l, "change", function() {
                    a = l.value
                });
                g.appendChild(l);
                k = document.createElement("div");
                k.style.marginTop = "6px";
                k.style.display = "inline-block";
                k.style.width = "140px";
                mxUtils.write(k, mxResources.get("parentChildSpacing") + ": ");
                g.appendChild(k);
                var p = document.createElement("input");
                p.type = "number";
                p.value = c;
                p.style.width = "200px";
                p.style.boxSizing = "border-box";
                g.appendChild(p);
                mxEvent.addListener(p, "change", function() {
                    c = p.value
                });
                k = document.createElement("div");
                k.style.marginTop = "6px";
                k.style.display = "inline-block";
                k.style.width = "140px";
                mxUtils.write(k, mxResources.get("siblingSpacing") + ": ");
                g.appendChild(k);
                var z = document.createElement("input");
                z.type = "number";
                z.value = d;
                z.style.width = "200px";
                z.style.boxSizing = "border-box";
                g.appendChild(z);
                mxEvent.addListener(z, "change", function() {
                    d = z.value
                });
                g = new CustomDialog(b, g, function() {
                    null == a && (a = 2);
                    "undefined" !== typeof mxOrgChartLayout || b.loadingOrgChart || b.isOffline(!0) ? f() : b.spinner.spin(document.body, mxResources.get("loading")) && (b.loadingOrgChart = !0, "1" == urlParams.dev ? mxscript("js/orgchart.min.js", f) : mxscript("js/extensions.min.js", f))
                });
                b.showDialog(g.container, 355, 125, !0, !0)
            }, c, null, f());
            a.addSeparator(c);
            b.menus.addMenuItem(a, "runLayout", c, null, null, mxResources.get("apply") + "...")
        };
        this.put("help", new Menu(mxUtils.bind(this, function(a, c) {
            if (!mxClient.IS_CHROMEAPP && b.isOffline()) this.addMenuItems(a, ["about"], c);
            else {
                var d = a.addItem("Search:", null, null, c, null, null, !1);
                d.style.backgroundColor = "dark" == uiTheme ? "#505759" : "whiteSmoke";
                d.style.cursor = "default";
                var e = document.createElement("input");
                e.setAttribute("type", "text");
                e.setAttribute("size", "25");
                e.style.marginLeft = "8px";
                mxEvent.addListener(e, "keydown", mxUtils.bind(this, function(a) {
                    var b = mxUtils.trim(e.value);
                    13 == a.keyCode && 0 < b.length ? (this.editorUi.openLink("https://desk.draw.io/support/search/solutions?term=" + encodeURIComponent(b)), e.value = "", EditorUi.logEvent({
                        category: "SEARCH-HELP",
                        action: "search",
                        label: b
                    }), null != this.editorUi.menubar && window.setTimeout(mxUtils.bind(this, function() {
                        this.editorUi.menubar.hideMenu()
                    }), 0)) : 27 == a.keyCode && (e.value = "")
                }));
                d.firstChild.nextSibling.appendChild(e);
                mxEvent.addGestureListeners(e, function(a) {
                    document.activeElement != e && e.focus();
                    mxEvent.consume(a)
                }, function(a) {
                    mxEvent.consume(a)
                }, function(a) {
                    mxEvent.consume(a)
                });
                window.setTimeout(function() {
                    e.focus()
                }, 0);
                this.addMenuItems(a, "- keyboardShortcuts quickStart support - about".split(" "), c)
            }
            "1" == urlParams.test && (a.addSeparator(c), this.addSubmenu("testDevelop", a, c))
        })));
        "1" == urlParams.test && (mxResources.parse("testDevelop=Develop"), mxResources.parse("showBoundingBox=Show bounding box"), mxResources.parse("createSidebarEntry=Create Sidebar Entry"), mxResources.parse("testCheckFile=Check File"), mxResources.parse("testDiff=Diff"), mxResources.parse("testInspect=Inspect"), mxResources.parse("testShowConsole=Show Console"), mxResources.parse("testXmlImageExport=XML Image Export"), mxResources.parse("testDownloadRtModel=Export RT model"), mxResources.parse("testImportRtModel=Import RT model"), b.actions.addAction("createSidebarEntry", mxUtils.bind(this, function() {
            if (!k.isSelectionEmpty()) {
                var a = k.cloneCells(k.getSelectionCells()),
                    c = k.getBoundingBoxFromGeometry(a),
                    a = k.moveCells(a, -c.x, -c.y);
                b.showTextDialog("Create Sidebar Entry", "this.addDataEntry('tag1 tag2', " + c.width + ", " + c.height + ", 'The Title', '" + Graph.compress(mxUtils.getXml(k.encodeCells(a))) + "'),")
            }
        })), b.actions.addAction("showBoundingBox", mxUtils.bind(this, function() {
            var a = k.getGraphBounds(),
                b = k.view.translate,
                c = k.view.scale;
            k.insertVertex(k.getDefaultParent(), null, "", a.x / c - b.x, a.y / c - b.y, a.width / c, a.height / c, "fillColor=none;strokeColor=red;")
        })), b.actions.addAction("testCheckFile", mxUtils.bind(this, function() {
            var a = null != b.pages && null != b.getCurrentFile() ? b.getCurrentFile().getAnonymizedXmlForPages(b.pages) : "",
                a = new TextareaDialog(b, "Paste Data:", a, function(a) {
                    if (0 < a.length) try {
                        var c = function(a) {
                            function b(a) {
                                if (null == n[a]) {
                                    if (n[a] = !0, null != e[a]) {
                                        for (; 0 < e[a].length;) {
                                            var d = e[a].pop();
                                            b(d)
                                        }
                                        delete e[a]
                                    }
                                } else mxLog.debug(c + ": Visited: " + a)
                            }
                            var c = a.parentNode.id,
                                d = a.childNodes;
                            a = {};
                            for (var e = {}, f = null, g = {}, k = 0; k < d.length; k++) {
                                var l = d[k];
                                if (null != l.id && 0 < l.id.length)
                                    if (null == a[l.id]) {
                                        a[l.id] = l.id;
                                        var m = l.getAttribute("parent");
                                        null == m ? null != f ? mxLog.debug(c + ": Multiple roots: " + l.id) : f = l.id : (null == e[m] && (e[m] = []), e[m].push(l.id))
                                    } else g[l.id] = l.id
                            }
                            0 < Object.keys(g).length ? (d = c + ": " + Object.keys(g).length + " Duplicates: " + Object.keys(g).join(", "), mxLog.debug(d + " (see console)")) : mxLog.debug(c + ": Checked");
                            var n = {};
                            null == f ? mxLog.debug(c + ": No root") : (b(f), Object.keys(n).length != Object.keys(a).length && (mxLog.debug(c + ": Invalid tree: (see console)"), console.log(c + ": Invalid tree", e)))
                        };
                        "<" != a.charAt(0) && (a = Graph.decompress(a), mxLog.debug("See console for uncompressed XML"), console.log("xml", a));
                        var d = mxUtils.parseXml(a),
                            e = b.getPagesForNode(d.documentElement, "mxGraphModel");
                        if (null != e && 0 < e.length) try {
                            var f = b.getHashValueForPages(e);
                            mxLog.debug("Checksum: ", f)
                        } catch (I) {
                            mxLog.debug("Error: ", I.message)
                        } else mxLog.debug("No pages found for checksum");
                        var g = d.getElementsByTagName("root");
                        for (a = 0; a < g.length; a++) c(g[a]);
                        mxLog.show()
                    } catch (I) {
                        b.handleError(I), null != window.console && console.error(I)
                    }
                });
            a.textarea.style.width = "600px";
            a.textarea.style.height = "380px";
            b.showDialog(a.container, 620, 460, !0, !0);
            a.init()
        })), b.actions.addAction("testDiff", mxUtils.bind(this, function() {
            if (null != b.pages) {
                var a = new TextareaDialog(b, "Paste Data:", "", function(a) {
                    if (0 < a.length) try {
                        console.log(JSON.stringify(b.diffPages(b.pages, b.getPagesForNode(mxUtils.parseXml(a).documentElement)), null, 2))
                    } catch (C) {
                        b.handleError(C), null != window.console && console.error(C)
                    }
                });
                a.textarea.style.width = "600px";
                a.textarea.style.height = "380px";
                b.showDialog(a.container, 620, 460, !0, !0);
                a.init()
            } else b.alert("No pages")
        })), b.actions.addAction("testInspect", mxUtils.bind(this, function() {
            console.log(b, k.getModel())
        })), b.actions.addAction("testXmlImageExport", mxUtils.bind(this, function() {
            var a = new mxImageExport,
                b = k.getGraphBounds(),
                c = k.view.scale,
                d = mxUtils.createXmlDocument(),
                e = d.createElement("output");
            d.appendChild(e);
            d = new mxXmlCanvas2D(e);
            d.translate(Math.floor((1 - b.x) / c), Math.floor((1 - b.y) / c));
            d.scale(1 / c);
            var f = 0,
                g = d.save;
            d.save = function() {
                f++;
                g.apply(this, arguments)
            };
            var l = d.restore;
            d.restore = function() {
                f--;
                l.apply(this, arguments)
            };
            var m = a.drawShape;
            a.drawShape = function(a) {
                mxLog.debug("entering shape", a, f);
                m.apply(this, arguments);
                mxLog.debug("leaving shape", a, f)
            };
            a.drawState(k.getView().getState(k.model.root), d);
            mxLog.show();
            mxLog.debug(mxUtils.getXml(e));
            mxLog.debug("stateCounter", f)
        })), b.actions.addAction("testDownloadRtModel...", mxUtils.bind(this, function() {
            null == b.drive ? b.handleError({
                message: mxResources.get("serviceUnavailableOrBlocked")
            }) : b.drive.execute(mxUtils.bind(this, function() {
                var a = prompt("File ID", "");
                if (null != a && 0 < a.length && b.spinner.spin(document.body, mxResources.get("export"))) {
                    var c = new mxXmlRequest("https://www.googleapis.com/drive/v2/files/" + a + "/realtime?supportsAllDrives=true", null, "GET");
                    c.setRequestHeaders = function(a) {
                        mxXmlRequest.prototype.setRequestHeaders.apply(this, arguments);
                        a.setRequestHeader("authorization", "Bearer " + b.drive.token)
                    };
                    c.send(function(c) {
                        b.spinner.stop();
                        200 <= c.getStatus() && 299 >= c.getStatus() ? b.saveLocalFile(c.getText(), "json-" + a + ".txt", "text/plain") : b.handleError({
                            message: mxResources.get("fileNotFound")
                        }, mxResources.get("errorLoadingFile"))
                    })
                }
            }))
        })), b.actions.addAction("testShowConsole", function() {
            mxLog.isVisible() ? mxLog.window.fit() : mxLog.show();
            mxLog.window.div.style.zIndex = mxPopupMenu.prototype.zIndex - 1
        }), this.put("testDevelop", new Menu(mxUtils.bind(this, function(a, c) {
            this.addMenuItems(a, "createSidebarEntry showBoundingBox - testCheckFile testDiff - testInspect - testXmlImageExport - testDownloadRtModel".split(" "), c);
            a.addItem(mxResources.get("testImportRtModel") + "...", null, function() {
                var a = document.createElement("input");
                a.setAttribute("type", "file");
                mxEvent.addListener(a, "change", mxUtils.bind(this, function() {
                    if (null != a.files) {
                        var c = new FileReader;
                        c.onload = mxUtils.bind(this, function(c) {
                            try {
                                b.openLocalFile(mxUtils.getXml(b.drive.convertJsonToXml(JSON.parse(c.target.result).data)), a.files[0].name, !0)
                            } catch (F) {
                                b.handleError(F, mxResources.get("errorLoadingFile"))
                            }
                        });
                        c.readAsText(a.files[0])
                    }
                }));
                a.click()
            }, c);
            this.addMenuItems(a, ["-", "testShowConsole"], c)
        }))));
        b.actions.addAction("shapes...", function() {
            mxClient.IS_CHROMEAPP || !b.isOffline() ? b.showDialog((new MoreShapesDialog(b, !0)).container, 640, isLocalStorage ? mxClient.IS_IOS ? 480 : 460 : 440, !0, !0) : b.showDialog((new MoreShapesDialog(b, !1)).container, 360, isLocalStorage ? mxClient.IS_IOS ? 300 : 280 : 260, !0, !0)
        });
        b.actions.put("createShape", new Action(mxResources.get("shape") + "...", function(a) {
            k.isEnabled() && (a = new mxCell("", new mxGeometry(0, 0, 120, 120), b.defaultCustomShapeStyle), a.vertex = !0, a = new EditShapeDialog(b, a, mxResources.get("editShape") + ":", 630, 400), b.showDialog(a.container, 640, 480, !0, !1), a.init())
        })).isEnabled = f;
        b.actions.put("embedHtml", new Action(mxResources.get("html") + "...", function() {
            b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(a) {
                b.spinner.stop();
                b.showHtmlDialog(mxResources.get("create"), "https://desk.draw.io/support/solutions/articles/16000042542", a, function(a, c, d, e, f, g, k, l, m, n) {
                    b.createHtml(a, c, d, e, f, g, k, l, m, n, mxUtils.bind(this, function(a, c) {
                        var d = new EmbedDialog(b, a + "\n" + c, null, null, function() {
                            var d = window.open(),
                                e = d.document;
                            if (null != e) {
                                "CSS1Compat" === document.compatMode && e.writeln("<!DOCTYPE html>");
                                e.writeln("<html>");
                                e.writeln("<head><title>" + encodeURIComponent(mxResources.get("preview")) + '</title><meta charset="utf-8"></head>');
                                e.writeln("<body>");
                                e.writeln(a);
                                var f = mxClient.IS_IE || mxClient.IS_EDGE || null != document.documentMode;
                                f && e.writeln(c);
                                e.writeln("</body>");
                                e.writeln("</html>");
                                e.close();
                                if (!f) {
                                    var g = d.document.createElement("div");
                                    g.marginLeft = "26px";
                                    g.marginTop = "26px";
                                    mxUtils.write(g, mxResources.get("updatingDocument"));
                                    f = d.document.createElement("img");
                                    f.setAttribute("src", window.location.protocol + "//" + window.location.hostname + "/" + IMAGE_PATH + "/spin.gif");
                                    f.style.marginLeft = "6px";
                                    g.appendChild(f);
                                    d.document.body.insertBefore(g, d.document.body.firstChild);
                                    window.setTimeout(function() {
                                        var a = document.createElement("script");
                                        a.type = "text/javascript";
                                        a.src = /<script.*?src="(.*?)"/.exec(c)[1];
                                        e.body.appendChild(a);
                                        g.parentNode.removeChild(g)
                                    }, 20)
                                }
                            } else b.handleError({
                                message: mxResources.get("errorUpdatingPreview")
                            })
                        });
                        b.showDialog(d.container, 440, 240, !0, !0);
                        d.init()
                    }))
                })
            })
        }));
        b.actions.put("liveImage", new Action("Live image...", function() {
            var a = b.getCurrentFile();
            null != a && b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(c) {
                b.spinner.stop();
                null != c ? (c = new EmbedDialog(b, '<img src="' + (a.constructor != DriveFile ? c : "https://drive.google.com/uc?id=" + a.getId()) + '"/>'), b.showDialog(c.container, 440, 240, !0, !0), c.init()) : b.handleError({
                    message: mxResources.get("invalidPublicUrl")
                })
            })
        }));
        b.actions.put("embedImage", new Action(mxResources.get("image") + "...", function() {
            b.showEmbedImageDialog(function(a, c, d, e, f, g) {
                b.spinner.spin(document.body, mxResources.get("loading")) && b.createEmbedImage(a, c, d, e, f, g, function(a) {
                    b.spinner.stop();
                    a = new EmbedDialog(b, a);
                    b.showDialog(a.container, 440, 240, !0, !0);
                    a.init()
                }, function(a) {
                    b.spinner.stop();
                    b.handleError(a)
                })
            }, mxResources.get("image"), mxResources.get("retina"), b.isExportToCanvas())
        }));
        b.actions.put("embedSvg", new Action(mxResources.get("formatSvg") + "...", function() {
            b.showEmbedImageDialog(function(a, c, d, e, f, g) {
                b.spinner.spin(document.body, mxResources.get("loading")) && b.createEmbedSvg(a, c, d, e, f, g, function(a) {
                    b.spinner.stop();
                    a = new EmbedDialog(b, a);
                    b.showDialog(a.container, 440, 240, !0, !0);
                    a.init()
                }, function(a) {
                    b.spinner.stop();
                    b.handleError(a)
                })
            }, mxResources.get("formatSvg"), mxResources.get("image"), !0, "https://desk.draw.io/support/solutions/articles/16000042548")
        }));
        b.actions.put("embedIframe", new Action(mxResources.get("iframe") + "...", function() {
            var a = k.getGraphBounds();
            b.showPublishLinkDialog(mxResources.get("iframe"), null, "100%", Math.ceil((a.y + a.height - k.view.translate.y) / k.view.scale) + 2, function(a, c, d, e, f, g, k, l) {
                b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(m) {
                    b.spinner.stop();
                    m = new EmbedDialog(b, '<iframe frameborder="0" style="width:' + k + ";height:" + l + ';" src="' + b.createLink(a, c, d, e, f, g, m) + '"></iframe>');
                    b.showDialog(m.container, 440, 240, !0, !0);
                    m.init()
                })
            }, !0)
        }));
        b.actions.put("publishLink", new Action(mxResources.get("link") + "...", function() {
            b.showPublishLinkDialog(null, null, null, null, function(a, c, d, e, f, g) {
                b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(k) {
                    b.spinner.stop();
                    k = new EmbedDialog(b, b.createLink(a, c, d, e, f, g, k));
                    b.showDialog(k.container, 440, 240, !0, !0);
                    k.init()
                })
            })
        }));
        b.actions.addAction("microsoftOffice...", function() {
            b.openLink("https://office.draw.io")
        });
        b.actions.addAction("googleDocs...", function() {
            b.openLink("http://docsaddon.draw.io")
        });
        b.actions.addAction("googleSlides...", function() {
            b.openLink("https://slidesaddon.draw.io")
        });
        b.actions.addAction("googleSheets...", function() {
            b.openLink("https://sheetsaddon.draw.io")
        });
        b.actions.addAction("googleSites...", function() {
            b.spinner.spin(document.body, mxResources.get("loading")) && b.getPublicUrl(b.getCurrentFile(), function(a) {
                b.spinner.stop();
                a = new GoogleSitesDialog(b, a);
                b.showDialog(a.container, 420, 256, !0, !0);
                a.init()
            })
        });
        if (isLocalStorage || mxClient.IS_CHROMEAPP) n = b.actions.addAction("scratchpad", function() {
            b.toggleScratchpad()
        }), n.setToggleAction(!0), n.setSelectedCallback(function() {
            return null != b.scratchpad
        }), b.actions.addAction("plugins...", function() {
            b.showDialog((new PluginsDialog(b)).container, 360, 170, !0, !1)
        });
        n = b.actions.addAction("search", function() {
            var a = b.sidebar.isEntryVisible("search");
            b.sidebar.showPalette("search", !a);
            isLocalStorage && (mxSettings.settings.search = !a, mxSettings.save())
        });
        n.setToggleAction(!0);
        n.setSelectedCallback(function() {
            return b.sidebar.isEntryVisible("search")
        });
        "1" == urlParams.embed && (b.actions.get("save").funct = function(a) {
            k.isEditing() && k.stopEditing();
            var c = "0" != urlParams.pages || null != b.pages && 1 < b.pages.length ? b.getFileData(!0) : mxUtils.getXml(b.editor.getGraphXml());
            if ("json" == urlParams.proto) {
                var d = b.createLoadMessage("save");
                d.xml = c;
                a && (d.exit = !0);
                c = JSON.stringify(d)
            }(window.opener || window.parent).postMessage(c, "*");
            "0" != urlParams.modified && "1" != urlParams.keepmodified && (b.editor.modified = !1, b.editor.setStatus(""));
            a = b.getCurrentFile();
            null == a || a.constructor == LocalFile && null == a.mode || b.saveFile()
        }, b.actions.addAction("saveAndExit", function() {
            b.actions.get("save").funct(!0)
        }), b.actions.addAction("exit", function() {
            var a = function() {
                b.editor.modified = !1;
                var a = "json" == urlParams.proto ? JSON.stringify({
                    event: "exit",
                    modified: b.editor.modified
                }) : "";
                (window.opener || window.parent).postMessage(a, "*")
            };
            b.editor.modified ? b.confirm(mxResources.get("allChangesLost"), null, a, mxResources.get("cancel"), mxResources.get("discardChanges")) : a()
        }));
        this.put("exportAs", new Menu(mxUtils.bind(this, function(a, c) {
            b.isExportToCanvas() ? (this.addMenuItems(a, ["exportPng"], c), b.jpgSupported && this.addMenuItems(a, ["exportJpg"], c)) : b.isOffline() || mxClient.IS_IOS && navigator.standalone || this.addMenuItems(a, ["exportPng", "exportJpg"], c);
            this.addMenuItems(a, ["exportSvg", "-"], c);
            b.isOffline() || b.printPdfExport ? this.addMenuItems(a, ["exportPdf"], c) : b.isOffline() || mxClient.IS_IOS && navigator.standalone || this.addMenuItems(a, ["exportPdf"], c);
            mxClient.IS_IE || "undefined" === typeof VsdxExport && b.isOffline() || this.addMenuItems(a, ["exportVsdx"], c);
            this.addMenuItems(a, ["-", "exportHtml", "exportXml", "exportUrl"], c);
            b.isOffline() || (a.addSeparator(c), this.addMenuItem(a, "export", c).firstChild.nextSibling.innerHTML = mxResources.get("advanced") + "...")
        })));
        this.put("importFrom", new Menu(mxUtils.bind(this, function(a, c) {
            function e(a) {
                a.pickFile(function(c) {
                    b.spinner.spin(document.body, mxResources.get("loading")) && a.getFile(c, function(a) {
                        var c = "data:image/" == a.getData().substring(0, 11) ? n(a.getTitle()) : "text/xml";
                        /\.svg$/i.test(a.getTitle()) && !b.editor.isDataSvg(a.getData()) && (a.setData(Editor.createSvgDataUri(a.getData())), c = "image/svg+xml");
                        f(a.getData(), c, a.getTitle())
                    }, function(a) {
                        b.handleError(a, null != a ? mxResources.get("errorLoadingFile") : null)
                    }, a == b.drive)
                }, !0)
            }
            var f = mxUtils.bind(this, function(a, c, d) {
                    var e = k.view,
                        f = k.getGraphBounds(),
                        g = k.snap(Math.ceil(Math.max(0, f.x / e.scale - e.translate.x) + 4 * k.gridSize)),
                        l = k.snap(Math.ceil(Math.max(0, (f.y + f.height) / e.scale - e.translate.y) + 4 * k.gridSize));
                    "data:image/" == a.substring(0, 11) ? b.loadImage(a, mxUtils.bind(this, function(e) {
                        var f = !0,
                            m = mxUtils.bind(this, function() {
                                b.resizeImage(e, a, mxUtils.bind(this, function(e, m, n) {
                                    e = f ? Math.min(1, Math.min(b.maxImageSize / m, b.maxImageSize / n)) : 1;
                                    b.importFile(a, c, g, l, Math.round(m * e), Math.round(n * e), d, function(a) {
                                        b.spinner.stop();
                                        k.setSelectionCells(a);
                                        k.scrollCellToVisible(k.getSelectionCell())
                                    })
                                }), f)
                            });
                        a.length > b.resampleThreshold ? b.confirmImageResize(function(a) {
                            f = a;
                            m()
                        }) : m()
                    }), mxUtils.bind(this, function() {
                        b.handleError({
                            message: mxResources.get("cannotOpenFile")
                        })
                    })) : b.importFile(a, c, g, l, 0, 0, d, function(a) {
                        b.spinner.stop();
                        k.setSelectionCells(a);
                        k.scrollCellToVisible(k.getSelectionCell())
                    })
                }),
                n = mxUtils.bind(this, function(a) {
                    var b = "text/xml";
                    /\.png$/i.test(a) ? b = "image/png" : /\.jpe?g$/i.test(a) ? b = "image/jpg" : /\.gif$/i.test(a) ? b = "image/gif" : /\.pdf$/i.test(a) && (b = "application/pdf");
                    return b
                });
            "undefined" != typeof google && "undefined" != typeof google.picker && (null != b.drive ? a.addItem(mxResources.get("googleDrive") + "...", null, function() {
                e(b.drive)
            }, c) : l && "function" === typeof window.DriveClient && a.addItem(mxResources.get("googleDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1));
            null != b.oneDrive ? a.addItem(mxResources.get("oneDrive") + "...", null, function() {
                e(b.oneDrive)
            }, c) : g && "function" === typeof window.OneDriveClient && a.addItem(mxResources.get("oneDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            null != b.dropbox ? a.addItem(mxResources.get("dropbox") + "...", null, function() {
                e(b.dropbox)
            }, c) : d && "function" === typeof window.DropboxClient && a.addItem(mxResources.get("dropbox") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            null != b.gitHub && a.addItem(mxResources.get("github") + "...", null, function() {
                e(b.gitHub)
            }, c);
            null != b.gitLab && a.addItem(mxResources.get("gitlab") + "...", null, function() {
                e(b.gitLab)
            }, c);
            null != b.trello ? a.addItem(mxResources.get("trello") + "...", null, function() {
                e(b.trello)
            }, c) : m && "function" === typeof window.TrelloClient && a.addItem(mxResources.get("trello") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            isLocalStorage && "0" != urlParams.browser && a.addItem(mxResources.get("browser") + "...", null, function() {
                b.importLocalFile(!1)
            }, c);
            a.addItem(mxResources.get("device") + "...", null, function() {
                b.importLocalFile(!0)
            }, c);
            b.isOffline() || (a.addSeparator(c), a.addItem(mxResources.get("url") + "...", null, function() {
                var a = new FilenameDialog(b, "", mxResources.get("import"), function(a) {
                    if (null != a && 0 < a.length && b.spinner.spin(document.body, mxResources.get("loading"))) {
                        var c = /(\.png)($|\?)/i.test(a) ? "image/png" : "text/xml";
                        b.editor.loadUrl(PROXY_URL + "?url=" + encodeURIComponent(a), function(b) {
                            f(b, c, a)
                        }, function() {
                            b.spinner.stop();
                            b.handleError(null, mxResources.get("errorLoadingFile"))
                        }, "image/png" == c)
                    }
                }, mxResources.get("url"));
                b.showDialog(a.container, 300, 80, !0, !0);
                a.init()
            }, c))
        }))).isEnabled = f;
        this.put("theme", new Menu(mxUtils.bind(this, function(a, c) {
            var d = mxSettings.getUi(),
                e = a.addItem(mxResources.get("automatic"), null, function() {
                    mxSettings.setUi("");
                    mxSettings.save();
                    b.alert(mxResources.get("restartForChangeRequired"))
                }, c);
            "kennedy" != d && "atlas" != d && "dark" != d && "min" != d && a.addCheckmark(e, Editor.checkmarkImage);
            a.addSeparator(c);
            e = a.addItem(mxResources.get("kennedy"), null, function() {
                mxSettings.setUi("kennedy");
                mxSettings.save();
                b.alert(mxResources.get("restartForChangeRequired"))
            }, c);
            "kennedy" == d && a.addCheckmark(e, Editor.checkmarkImage);
            e = a.addItem(mxResources.get("minimal"), null, function() {
                mxSettings.setUi("min");
                mxSettings.save();
                b.alert(mxResources.get("restartForChangeRequired"))
            }, c);
            "min" == d && a.addCheckmark(e, Editor.checkmarkImage);
            e = a.addItem(mxResources.get("atlas"), null, function() {
                mxSettings.setUi("atlas");
                mxSettings.save();
                b.alert(mxResources.get("restartForChangeRequired"))
            }, c);
            "atlas" == d && a.addCheckmark(e, Editor.checkmarkImage);
            e = a.addItem(mxResources.get("dark"), null, function() {
                mxSettings.setUi("dark");
                mxSettings.save();
                b.alert(mxResources.get("restartForChangeRequired"))
            }, c);
            "dark" == d && a.addCheckmark(e, Editor.checkmarkImage)
        })));
        n = this.editorUi.actions.addAction("rename...", mxUtils.bind(this, function() {
            var a = this.editorUi.getCurrentFile();
            if (null != a) {
                var c = null != a.getTitle() ? a.getTitle() : this.editorUi.defaultFilename,
                    c = new FilenameDialog(this.editorUi, c, mxResources.get("rename"), mxUtils.bind(this, function(b) {
                        null != b && 0 < b.length && null != a && b != a.getTitle() && this.editorUi.spinner.spin(document.body, mxResources.get("renaming")) && a.rename(b, mxUtils.bind(this, function(a) {
                            this.editorUi.spinner.stop()
                        }), mxUtils.bind(this, function(a) {
                            this.editorUi.handleError(a, null != a ? mxResources.get("errorRenamingFile") : null)
                        }))
                    }), a.constructor == DriveFile || a.constructor == StorageFile ? mxResources.get("diagramName") : null, function(a) {
                        if (null != a && 0 < a.length) return !0;
                        b.showError(mxResources.get("error"), mxResources.get("invalidName"), mxResources.get("ok"));
                        return !1
                    }, null, null, null, null, b.editor.fileExtensions);
                this.editorUi.showDialog(c.container, 340, 90, !0, !0);
                c.init()
            }
        }));
        n.isEnabled = function() {
            return this.enabled && f.apply(this, arguments)
        };
        n.visible = "1" != urlParams.embed;
        b.actions.addAction("makeCopy...", mxUtils.bind(this, function() {
            var a = b.getCurrentFile();
            if (null != a) {
                var c = b.getCopyFilename(a);
                a.constructor == DriveFile ? (c = new CreateDialog(b, c, mxUtils.bind(this, function(c, d) {
                    "download" == d && (d = App.MODE_GOOGLE);
                    null != c && 0 < c.length && (d == App.MODE_GOOGLE ? b.spinner.spin(document.body, mxResources.get("saving")) && a.saveAs(c, mxUtils.bind(this, function(c) {
                        a.desc = c;
                        a.save(!1, mxUtils.bind(this, function() {
                            b.spinner.stop();
                            a.setModified(!1);
                            a.addAllSavedStatus()
                        }), mxUtils.bind(this, function(a) {
                            b.handleError(a)
                        }))
                    }), mxUtils.bind(this, function(a) {
                        b.handleError(a)
                    })) : b.createFile(c, b.getFileData(!0), null, d))
                }), mxUtils.bind(this, function() {
                    b.hideDialog()
                }), mxResources.get("makeCopy"), mxResources.get("create"), null, null, null, null, !0, null, null, null, null, b.editor.fileExtensions), b.showDialog(c.container, 420, 380, !0, !0), c.init()) : b.editor.editAsNew(this.editorUi.getFileData(!0), c)
            }
        }));
        b.actions.addAction("moveToFolder...", mxUtils.bind(this, function() {
            var a = b.getCurrentFile();
            if (a.getMode() == App.MODE_GOOGLE || a.getMode() == App.MODE_ONEDRIVE) {
                var c = !1;
                if (a.getMode() == App.MODE_GOOGLE && null != a.desc.parents)
                    for (var d = 0; d < a.desc.parents.length; d++)
                        if (a.desc.parents[d].isRoot) {
                            c = !0;
                            break
                        }
                b.pickFolder(a.getMode(), mxUtils.bind(this, function(c) {
                    b.spinner.spin(document.body, mxResources.get("moving")) && a.move(c, mxUtils.bind(this, function(a) {
                        b.spinner.stop()
                    }), mxUtils.bind(this, function(a) {
                        b.handleError(a)
                    }))
                }), null, !0, c)
            }
        }));
        this.put("publish", new Menu(mxUtils.bind(this, function(a, b) {
            this.addMenuItems(a, ["publishLink"], b)
        })));
        b.actions.put("useOffline", new Action(mxResources.get("useOffline") + "...", function() {
            b.openLink("https://app.draw.io/")
        }));
        b.actions.put("downloadDesktop", new Action(mxResources.get("downloadDesktop") + "...", function() {
            b.openLink("https://get.draw.io/")
        }));
        this.editorUi.actions.addAction("share...", mxUtils.bind(this, function() {
            try {
                var a = b.getCurrentFile();
                null != a && a.share()
            } catch (B) {
                b.handleError(B)
            }
        }));
        this.put("embed", new Menu(mxUtils.bind(this, function(a, c) {
            var d = b.getCurrentFile();
            null == d || d.getMode() != App.MODE_GOOGLE && d.getMode() != App.MODE_GITHUB || !/(\.png)$/i.test(d.getTitle()) || this.addMenuItems(a, ["liveImage", "-"], c);
            this.addMenuItems(a, ["embedImage", "embedSvg", "-", "embedHtml"], c);
            navigator.standalone || b.isOffline() || this.addMenuItems(a, ["embedIframe"], c);
            "1" == urlParams.embed || b.isOffline() || this.addMenuItems(a, "- googleDocs googleSlides googleSheets - microsoftOffice".split(" "), c)
        })));
        var v = function(a, c, d, e) {
                ("plantUml" != e || EditorUi.enablePlantUml && !b.isOffline()) && a.addItem(d, null, mxUtils.bind(this, function() {
                    if ("fromText" == e || "formatSql" == e || "plantUml" == e || "mermaid" == e) {
                        var a = new ParseDialog(b, d, e);
                        b.showDialog(a.container, 620, 420, !0, !1);
                        b.dialog.container.style.overflow = "auto"
                    } else a = new CreateGraphDialog(b, d, e), b.showDialog(a.container, 620, 420, !0, !1);
                    a.init()
                }), c, null, f())
            },
            x = function(a, c, d, e) {
                var f = new mxCell(a, new mxGeometry(0, 0, c, d), e);
                f.vertex = !0;
                a = k.getCenterInsertPoint(k.getBoundingBoxFromGeometry([f], !0));
                f.geometry.x = a.x;
                f.geometry.y = a.y;
                k.getModel().beginUpdate();
                try {
                    f = k.addCell(f), k.fireEvent(new mxEventObject("cellsInserted", "cells", [f]))
                } finally {
                    k.getModel().endUpdate()
                }
                k.scrollCellToVisible(f);
                k.setSelectionCell(f);
                k.container.focus();
                k.editAfterInsert && k.startEditing(f);
                window.setTimeout(function() {
                    null != b.hoverIcons && b.hoverIcons.update(k.view.getState(f))
                }, 0);
                return f
            };
        b.actions.put("insertText", new Action(mxResources.get("text"), function() {
            k.isEnabled() && !k.isCellLocked(k.getDefaultParent()) && k.startEditingAtCell(x("Text", 40, 20, "text;html=1;resizable=0;autosize=1;align=center;verticalAlign=middle;points=[];fillColor=none;strokeColor=none;rounded=0;"))
        }), null, null, Editor.ctrlKey + "+Shift+X").isEnabled = f;
        b.actions.put("insertRectangle", new Action(mxResources.get("rectangle"), function() {
            k.isEnabled() && !k.isCellLocked(k.getDefaultParent()) && x("", 120, 60, "whiteSpace=wrap;html=1;")
        }), null, null, Editor.ctrlKey + "+K").isEnabled = f;
        b.actions.put("insertEllipse", new Action(mxResources.get("ellipse"), function() {
            k.isEnabled() && !k.isCellLocked(k.getDefaultParent()) && x("", 80, 80, "ellipse;whiteSpace=wrap;html=1;")
        }), null, null, Editor.ctrlKey + "+Shift+K").isEnabled = f;
        b.actions.put("insertRhombus", new Action(mxResources.get("rhombus"), function() {
            k.isEnabled() && !k.isCellLocked(k.getDefaultParent()) && x("", 80, 80, "rhombus;whiteSpace=wrap;html=1;")
        })).isEnabled = f;
        var y = mxUtils.bind(this, function(a, b, c) {
            for (var d = 0; d < c.length; d++) "-" == c[d] ? a.addSeparator(b) : v(a, b, mxResources.get(c[d]) + "...", c[d])
        });
        this.put("insert", new Menu(mxUtils.bind(this, function(a, c) {
            this.addMenuItems(a, "insertRectangle insertEllipse insertRhombus - insertText insertLink - createShape insertFreehand - insertImage".split(" "), c);
            b.insertTemplateEnabled && !b.isOffline() && this.addMenuItems(a, ["insertTemplate"], c);
            a.addSeparator(c);
            this.addSubmenu("insertLayout", a, c, mxResources.get("layout"));
            this.addSubmenu("insertAdvanced", a, c, mxResources.get("advanced"))
        })));
        this.put("insertLayout", new Menu(mxUtils.bind(this, function(a, b) {
            y(a, b, "horizontalFlow verticalFlow - horizontalTree verticalTree radialTree - organic circle".split(" "))
        })));
        this.put("insertAdvanced", new Menu(mxUtils.bind(this, function(a, c) {
            y(a, c, ["fromText", "plantUml", "mermaid", "-", "formatSql"]);
            a.addItem(mxResources.get("csv") + "...", null, function() {
                b.showImportCsvDialog()
            }, c, null, f())
        })));
        this.put("openRecent", new Menu(function(a, c) {
            var d = b.getRecent();
            if (null != d) {
                for (var e = 0; e < d.length; e++)(function(d) {
                    var e = d.mode;
                    e == App.MODE_GOOGLE ? e = "googleDrive" : e == App.MODE_ONEDRIVE && (e = "oneDrive");
                    a.addItem(d.title + " (" + mxResources.get(e) + ")", null, function() {
                        b.loadFile(d.id)
                    }, c)
                })(d[e]);
                a.addSeparator(c)
            }
            a.addItem(mxResources.get("reset"), null, function() {
                b.resetRecent()
            }, c)
        }));
        this.put("openFrom", new Menu(function(a, c) {
            null != b.drive ? a.addItem(mxResources.get("googleDrive") + "...", null, function() {
                b.pickFile(App.MODE_GOOGLE)
            }, c) : l && "function" === typeof window.DriveClient && a.addItem(mxResources.get("googleDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            null != b.oneDrive ? a.addItem(mxResources.get("oneDrive") + "...", null, function() {
                b.pickFile(App.MODE_ONEDRIVE)
            }, c) : g && "function" === typeof window.OneDriveClient && a.addItem(mxResources.get("oneDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            null != b.dropbox ? a.addItem(mxResources.get("dropbox") + "...", null, function() {
                b.pickFile(App.MODE_DROPBOX)
            }, c) : d && "function" === typeof window.DropboxClient && a.addItem(mxResources.get("dropbox") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            null != b.gitHub && a.addItem(mxResources.get("github") + "...", null, function() {
                b.pickFile(App.MODE_GITHUB)
            }, c);
            null != b.gitLab && a.addItem(mxResources.get("gitlab") + "...", null, function() {
                b.pickFile(App.MODE_GITLAB)
            }, c);
            null != b.trello ? a.addItem(mxResources.get("trello") + "...", null, function() {
                b.pickFile(App.MODE_TRELLO)
            }, c) : m && "function" === typeof window.TrelloClient && a.addItem(mxResources.get("trello") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            isLocalStorage && "0" != urlParams.browser && a.addItem(mxResources.get("browser") + "...", null, function() {
                b.pickFile(App.MODE_BROWSER)
            }, c);
            a.addItem(mxResources.get("device") + "...", null, function() {
                b.pickFile(App.MODE_DEVICE)
            }, c);
            b.isOffline() || (a.addSeparator(c), a.addItem(mxResources.get("url") + "...", null, function() {
                var a = new FilenameDialog(b, "", mxResources.get("open"), function(a) {
                    null != a && 0 < a.length && (null == b.getCurrentFile() ? window.location.hash = "#U" + encodeURIComponent(a) : window.openWindow((mxClient.IS_CHROMEAPP ? "https://www.draw.io/" : "https://" + location.host + "/") + window.location.search + "#U" + encodeURIComponent(a)))
                }, mxResources.get("url"));
                b.showDialog(a.container, 300, 80, !0, !0);
                a.init()
            }, c))
        }));
        Editor.enableCustomLibraries && (this.put("newLibrary", new Menu(function(a, c) {
            "undefined" != typeof google && "undefined" != typeof google.picker && (null != b.drive ? a.addItem(mxResources.get("googleDrive") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_GOOGLE)
            }, c) : l && "function" === typeof window.DriveClient && a.addItem(mxResources.get("googleDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1));
            null != b.oneDrive ? a.addItem(mxResources.get("oneDrive") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_ONEDRIVE)
            }, c) : g && "function" === typeof window.OneDriveClient && a.addItem(mxResources.get("oneDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            null != b.dropbox ? a.addItem(mxResources.get("dropbox") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_DROPBOX)
            }, c) : d && "function" === typeof window.DropboxClient && a.addItem(mxResources.get("dropbox") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            null != b.gitHub && a.addItem(mxResources.get("github") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_GITHUB)
            }, c);
            null != b.gitLab && a.addItem(mxResources.get("gitlab") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_GITLAB)
            }, c);
            null != b.trello ? a.addItem(mxResources.get("trello") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_TRELLO)
            }, c) : m && "function" === typeof window.TrelloClient && a.addItem(mxResources.get("trello") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            isLocalStorage && "0" != urlParams.browser && a.addItem(mxResources.get("browser") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_BROWSER)
            }, c);
            a.addItem(mxResources.get("device") + "...", null, function() {
                b.showLibraryDialog(null, null, null, null, App.MODE_DEVICE)
            }, c)
        })), this.put("openLibraryFrom", new Menu(function(a, c) {
            "undefined" != typeof google && "undefined" != typeof google.picker && (null != b.drive ? a.addItem(mxResources.get("googleDrive") + "...", null, function() {
                b.pickLibrary(App.MODE_GOOGLE)
            }, c) : l && "function" === typeof window.DriveClient && a.addItem(mxResources.get("googleDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1));
            null != b.oneDrive ? a.addItem(mxResources.get("oneDrive") + "...", null, function() {
                b.pickLibrary(App.MODE_ONEDRIVE)
            }, c) : g && "function" === typeof window.OneDriveClient && a.addItem(mxResources.get("oneDrive") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            null != b.dropbox ? a.addItem(mxResources.get("dropbox") + "...", null, function() {
                b.pickLibrary(App.MODE_DROPBOX)
            }, c) : d && "function" === typeof window.DropboxClient && a.addItem(mxResources.get("dropbox") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            null != b.gitHub && a.addItem(mxResources.get("github") + "...", null, function() {
                b.pickLibrary(App.MODE_GITHUB)
            }, c);
            null != b.gitLab && a.addItem(mxResources.get("gitlab") + "...", null, function() {
                b.pickLibrary(App.MODE_GITLAB)
            }, c);
            null != b.trello ? a.addItem(mxResources.get("trello") + "...", null, function() {
                b.pickLibrary(App.MODE_TRELLO)
            }, c) : m && "function" === typeof window.TrelloClient && a.addItem(mxResources.get("trello") + " (" + mxResources.get("loading") + "...)", null, function() {}, c, null, !1);
            a.addSeparator(c);
            isLocalStorage && "0" != urlParams.browser && a.addItem(mxResources.get("browser") + "...", null, function() {
                b.pickLibrary(App.MODE_BROWSER)
            }, c);
            a.addItem(mxResources.get("device") + "...", null, function() {
                b.pickLibrary(App.MODE_DEVICE)
            }, c);
            b.isOffline() || (a.addSeparator(c), a.addItem(mxResources.get("url") + "...", null, function() {
                var a = new FilenameDialog(b, "", mxResources.get("open"), function(a) {
                    if (null != a && 0 < a.length && b.spinner.spin(document.body, mxResources.get("loading"))) {
                        var c = a;
                        b.editor.isCorsEnabledForUrl(a) || (c = PROXY_URL + "?url=" + encodeURIComponent(a));
                        mxUtils.get(c, function(c) {
                            if (200 <= c.getStatus() && 299 >= c.getStatus()) {
                                b.spinner.stop();
                                try {
                                    b.loadLibrary(new UrlLibrary(this, c.getText(), a))
                                } catch (H) {
                                    b.handleError(H, mxResources.get("errorLoadingFile"))
                                }
                            } else b.spinner.stop(),
                                b.handleError(null, mxResources.get("errorLoadingFile"))
                        }, function() {
                            b.spinner.stop();
                            b.handleError(null, mxResources.get("errorLoadingFile"))
                        })
                    }
                }, mxResources.get("url"));
                b.showDialog(a.container, 300, 80, !0, !0);
                a.init()
            }, c));
            "1" == urlParams.confLib && (a.addSeparator(c), a.addItem(mxResources.get("confluenceCloud") + "...", null, function() {
                b.showRemotelyStoredLibrary(mxResources.get("libraries"))
            }, c))
        })));
        this.put("edit", new Menu(mxUtils.bind(this, function(a, b) {
            this.addMenuItems(a, ["undo", "redo", "-", "cut", "copy"]);
            EditorUi.isElectronApp && this.addMenuItems(a, ["copyAsImage"]);
            this.addMenuItems(a, "paste delete - duplicate - find - editData editTooltip - editStyle editGeometry - edit - editLink openLink - selectVertices selectEdges selectAll selectNone - lockUnlock".split(" "))
        })));
        n = b.actions.addAction("comments", mxUtils.bind(this, function() {
            if (null == this.commentsWindow) this.commentsWindow = new CommentsWindow(b, document.body.offsetWidth - 380, 120, 300, 350), this.commentsWindow.window.addListener("show", function() {
                    b.fireEvent(new mxEventObject("comments"))
                }),
                this.commentsWindow.window.addListener("hide", function() {
                    b.fireEvent(new mxEventObject("comments"))
                }), this.commentsWindow.window.setVisible(!0), b.fireEvent(new mxEventObject("comments"));
            else {
                var a = !this.commentsWindow.window.isVisible();
                this.commentsWindow.window.setVisible(a);
                this.commentsWindow.refreshCommentsTime();
                a && this.commentsWindow.hasError && this.commentsWindow.refreshComments()
            }
        }));
        n.setToggleAction(!0);
        n.setSelectedCallback(mxUtils.bind(this, function() {
            return null != this.commentsWindow && this.commentsWindow.window.isVisible()
        }));
        b.editor.addListener("fileLoaded", mxUtils.bind(this, function() {
            null != this.commentsWindow && (this.commentsWindow.destroy(), this.commentsWindow = null)
        }));
        var n = this.get("viewPanels"),
            A = n.funct;
        n.funct = function(a, c) {
            A.apply(this, arguments);
            b.commentsSupported() && b.menus.addMenuItems(a, ["comments"], c)
        };
        this.put("view", new Menu(mxUtils.bind(this, function(a, c) {
            this.addMenuItems(a, (null != this.editorUi.format ? ["formatPanel"] : []).concat(["outline", "layers"]).concat(b.commentsSupported() ? ["comments", "-"] : ["-"]));
            this.addMenuItems(a, ["-", "search"], c);
            if (isLocalStorage || mxClient.IS_CHROMEAPP) {
                var d = this.addMenuItem(a, "scratchpad", c);
                (!b.isOffline() || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) && this.addLinkToItem(d, "https://desk.draw.io/support/solutions/articles/16000042367")
            }
            this.addMenuItems(a, ["shapes", "-", "pageView", "pageScale"]);
            this.addSubmenu("units", a, c);
            this.addMenuItems(a, "- scrollbars tooltips ruler - grid guides".split(" "), c);
            mxClient.IS_SVG && (null == document.documentMode || 9 < document.documentMode) && this.addMenuItem(a, "shadowVisible", c);
            this.addMenuItems(a, "- connectionArrows connectionPoints - resetView zoomIn zoomOut".split(" "), c)
        })));
        this.put("extras", new Menu(mxUtils.bind(this, function(a, c) {
            "1" != urlParams.embed && (this.addSubmenu("theme", a, c), a.addSeparator(c));
            if ("undefined" !== typeof MathJax) {
                var d = this.addMenuItem(a, "mathematicalTypesetting", c);
                (!b.isOffline() || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) && this.addLinkToItem(d, "https://desk.draw.io/support/solutions/articles/16000032875")
            }
            this.addMenuItems(a, ["copyConnect", "collapseExpand", "-"], c);
            "1" != urlParams.embed && (isLocalStorage || mxClient.IS_CHROMEAPP) && this.addMenuItems(a, ["showStartScreen"], c);
            "1" != urlParams.embed && this.addMenuItems(a, ["autosave"], c);
            a.addSeparator(c);
            !b.isOfflineApp() && isLocalStorage && this.addMenuItem(a, "plugins", c);
            this.addMenuItems(a, ["tags", "-", "editDiagram", "-", "configuration"], c);
            a.addSeparator(c);
            "1" == urlParams.newTempDlg && (b.actions.addAction("templates", function() {
                var a = new TemplatesDialog;
                b.showDialog(a.container, a.width, a.height, !0, !1, null, !1, !0);
                a.init(b, function(a) {
                    console.log(a)
                }, null, null, null, "user", function(a, b) {
                    setTimeout(function() {
                        b ? a([{
                            url: "123",
                            title: "Test 1Test 1Test 1Test 1Test 1Test 1Test 11Test 1Test 11Test 1Test 1dgdsgdfg fdg dfgdfg dfg dfg"
                        }, {
                            url: "123",
                            title: "Test 2",
                            imgUrl: "https://www.google.com.eg/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png"
                        }, {
                            url: "123",
                            title: "Test 3",
                            changedBy: "Ashraf Teleb",
                            lastModifiedOn: "Yesterday"
                        }, {
                            url: "123",
                            title: "Test 4"
                        }, {
                            url: "123",
                            title: "Test 5"
                        }, {
                            url: "123",
                            title: "Test 6"
                        }]) : a([{
                            url: "123",
                            title: "Test 4",
                            imgUrl: "https://images.pexels.com/photos/459225/pexels-photo-459225.jpeg"
                        }, {
                            url: "123",
                            title: "Test 5"
                        }, {
                            url: "123",
                            title: "Test 6"
                        }, {
                            url: "123",
                            title: "Test 1Test 1Test 1Test 1Test 1Test 1Test 11Test 1Test 11Test 1Test 1dgdsgdfg fdg dfgdfg dfg dfg"
                        }, {
                            url: "123",
                            title: "Test 2",
                            imgUrl: "https://www.google.com.eg/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png"
                        }, {
                            url: "123",
                            title: "Test 3",
                            changedBy: "Ashraf Teleb",
                            lastModifiedOn: "Yesterday"
                        }]);
                        console.log(b)
                    }, 1E3)
                }, function(a, b, c) {
                    setTimeout(function() {
                        b(c ? [{
                            url: "123",
                            title: a + "Test 1Test 1Test 1Test 1Test 1Test 1Test 1"
                        }, {
                            url: "123",
                            title: a + "Test 2"
                        }, {
                            url: "123",
                            title: a + "Test 3"
                        }, {
                            url: "123",
                            title: a + "Test 4"
                        }, {
                            url: "123",
                            title: a + "Test 5"
                        }, {
                            url: "123",
                            title: a + "Test 6"
                        }] : [{
                            url: "123",
                            title: a + "Test 5"
                        }, {
                            url: "123",
                            title: a + "Test 6"
                        }, {
                            url: "123",
                            title: a + "Test 1Test 1Test 1Test 1Test 1Test 1Test 1"
                        }, {
                            url: "123",
                            title: a + "Test 2"
                        }, {
                            url: "123",
                            title: a + "Test 3"
                        }, {
                            url: "123",
                            title: a + "Test 4"
                        }])
                    }, 2E3)
                }, null)
            }), this.addMenuItem(a, "templates", c))
        })));
        this.put("file", new Menu(mxUtils.bind(this, function(a, c) {
            if ("1" == urlParams.embed) this.addSubmenu("importFrom", a, c), this.addMenuItems(a, ["-"], c), this.addSubmenu("exportAs", a, c), this.addSubmenu("embed", a, c), "1" == urlParams.libraries && (this.addMenuItems(a, ["-"], c), this.addSubmenu("newLibrary", a, c), this.addSubmenu("openLibraryFrom", a, c)), b.isRevisionHistorySupported() && this.addMenuItems(a, ["-", "revisionHistory"], c), this.addMenuItems(a, ["-", "pageSetup", "print", "-", "rename"], c), "1" == urlParams.noSaveBtn ? "0" != urlParams.saveAndExit && this.addMenuItems(a, ["saveAndExit"], c) : (this.addMenuItems(a, ["save"], c), "1" == urlParams.saveAndExit && this.addMenuItems(a, ["saveAndExit"], c)), "1" != urlParams.noExitBtn && this.addMenuItems(a, ["exit"], c);
            else {
                var d = this.editorUi.getCurrentFile();
                if (null != d && d.constructor == DriveFile) {
                    d.isRestricted() && this.addMenuItems(a, ["exportOptionsDisabled"], c);
                    this.addMenuItems(a, ["save", "-", "share"], c);
                    var e = this.addMenuItem(a, "synchronize", c);
                    (!b.isOffline() || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) && this.addLinkToItem(e, "https://desk.draw.io/support/solutions/articles/16000087947");
                    a.addSeparator(c)
                } else this.addMenuItems(a, ["new"], c);
                this.addSubmenu("openFrom", a, c);
                isLocalStorage && this.addSubmenu("openRecent", a, c);
                null != d && d.constructor == DriveFile ? this.addMenuItems(a, ["new", "-", "rename", "makeCopy", "moveToFolder"], c) : (mxClient.IS_CHROMEAPP || EditorUi.isElectronApp || null == d || d.constructor == LocalFile && null == d.fileHandle || (a.addSeparator(c), e = this.addMenuItem(a, "synchronize", c), (!b.isOffline() || mxClient.IS_CHROMEAPP || EditorUi.isElectronApp) && this.addLinkToItem(e, "https://desk.draw.io/support/solutions/articles/16000087947")), this.addMenuItems(a, ["-", "save", "saveAs", "-"], c), mxClient.IS_CHROMEAPP || EditorUi.isElectronApp || "draw.io" != b.getServiceName() || b.isOfflineApp() || null == d || this.addMenuItems(a, ["share", "-"], c), this.addMenuItems(a, ["rename"], c), b.isOfflineApp() ? navigator.onLine && "1" != urlParams.stealth && this.addMenuItems(a, ["upload"], c) : (this.addMenuItems(a, ["makeCopy"], c), null != d && d.constructor == OneDriveFile && this.addMenuItems(a, ["moveToFolder"], c)));
                a.addSeparator(c);
                this.addSubmenu("importFrom", a, c);
                this.addSubmenu("exportAs", a, c);
                a.addSeparator(c);
                this.addSubmenu("embed", a, c);
                this.addSubmenu("publish", a, c);
                a.addSeparator(c);
                this.addSubmenu("newLibrary", a, c);
                this.addSubmenu("openLibraryFrom", a, c);
                b.isRevisionHistorySupported() && this.addMenuItems(a, ["-", "revisionHistory"], c);
                null != d && null != b.fileNode && (d = null != d.getTitle() ? d.getTitle() : b.defaultFilename, /(\.html)$/i.test(d) || /(\.svg)$/i.test(d) || this.addMenuItems(a, ["-", "properties"]));
                this.addMenuItems(a, ["-", "pageSetup"], c);
                mxClient.IS_IOS && navigator.standalone || this.addMenuItems(a, ["print"], c);
                this.addMenuItems(a, ["-", "close"])
            }
        })));
        a.prototype.execute = function() {
            var a = this.ui.editor.graph;
            this.customFonts = this.prevCustomFonts;
            this.prevCustomFonts = this.ui.menus.customFonts;
            this.ui.fireEvent(new mxEventObject("customFontsChanged", "customFonts", this.customFonts));
            this.extFonts = this.previousExtFonts;
            for (var b = a.extFonts, c = 0; null != b && c < b.length; c++) {
                var d = document.getElementById("extFont_" + b[c].name);
                null != d && d.parentNode.removeChild(d)
            }
            a.extFonts = [];
            for (c = 0; null != this.previousExtFonts && c < this.previousExtFonts.length; c++) this.ui.editor.graph.addExtFont(this.previousExtFonts[c].name, this.previousExtFonts[c].url);
            this.previousExtFonts = b
        };
        this.put("fontFamily", new Menu(mxUtils.bind(this, function(b, c) {
            for (var d = mxUtils.bind(this, function(d, e, f) {
                    var g = this.editorUi.editor.graph,
                        k = this.styleChange(b, d, [mxConstants.STYLE_FONTFAMILY], [d], null, c, function() {
                            document.execCommand("fontname", !1, d);
                            g.addExtFont(d, e)
                        }, function() {
                            g.updateLabelElements(g.getSelectionCells(), function(a) {
                                a.removeAttribute("face");
                                a.style.fontFamily = null;
                                "PRE" == a.nodeName && g.replaceElement(a, "div")
                            });
                            g.addExtFont(d, e)
                        });
                    f && (f = document.createElement("span"), f.className = "geSprite geSprite-delete", f.style.cursor = "pointer", f.style.display = "inline-block", k.firstChild.nextSibling.nextSibling.appendChild(f), mxEvent.addListener(f, mxClient.IS_POINTER ? "pointerup" : "mouseup", mxUtils.bind(this, function(b) {
                        var c = mxUtils.clone(this.editorUi.editor.graph.extFonts);
                        if (null != c && 0 < c.length)
                            for (var e = 0; e < c.length; e++)
                                if (c[e].name == d) {
                                    c.splice(e, 1);
                                    break
                                }
                        for (var f = mxUtils.clone(this.customFonts), e = 0; e < f.length; e++)
                            if (f[e].name == d) {
                                f.splice(e, 1);
                                break
                            }
                        c = new a(this.editorUi, c, f);
                        this.editorUi.editor.graph.model.execute(c);
                        this.editorUi.menubar.hideMenu();
                        mxEvent.consume(b)
                    })));
                    k.firstChild.nextSibling.style.fontFamily = d
                }), e = 0; e < this.defaultFonts.length; e++) d(this.defaultFonts[e]);
            b.addSeparator(c);
            var f = this.editorUi.editor.graph.extFonts;
            if (null != f && 0 < f.length) {
                for (var g = {}, k = !1, e = 0; e < this.customFonts.length; e++) g[this.customFonts[e].name] = !0;
                for (e = 0; e < f.length; e++) g[f[e].name] || (this.customFonts.push(f[e]), k = !0);
                k && this.editorUi.fireEvent(new mxEventObject("customFontsChanged", "customFonts", this.customFonts))
            }
            if (0 < this.customFonts.length) {
                for (e = 0; e < this.customFonts.length; e++) f = this.customFonts[e].name, g = this.customFonts[e].url, d(f, g, !0), this.editorUi.editor.graph.addExtFont(f, g, !0);
                b.addSeparator(c);
                b.addItem(mxResources.get("reset"), null, mxUtils.bind(this, function() {
                    var b = new a(this.editorUi, [], []);
                    this.editorUi.editor.graph.model.execute(b)
                }), c);
                b.addSeparator(c)
            }
            b.addItem(mxResources.get("custom") + "...", null, mxUtils.bind(this, function() {
                var a = this.editorUi.editor.graph,
                    b = mxConstants.DEFAULT_FONTFAMILY,
                    c = "s",
                    d = null,
                    e = a.getView().getState(a.getSelectionCell());
                null != e && (b = e.style[mxConstants.STYLE_FONTFAMILY] || b, c = e.style.FType || c, "w" == c && (d = this.editorUi.editor.graph.extFonts, e = null, null != d && (e = d.find(function(a) {
                    return a.name == b
                })), d = null != e ? e.url : mxResources.get("urlNofFound", null, "URL not found"), 0 == d.indexOf(PROXY_URL) && (d = decodeURIComponent(d.substr((PROXY_URL + "?url=").length)))));
                c = new FontDialog(this.editorUi, b, d, c, mxUtils.bind(this, function(b, c, d) {
                    if (null != b && 0 < b.length) {
                        a.getModel().beginUpdate();
                        try {
                            a.stopEditing(!1);
                            a.setCellStyles(mxConstants.STYLE_FONTFAMILY, b);
                            "s" != d && (a.setCellStyles("FType", d), 0 == c.indexOf("http://") && (c = PROXY_URL + "?url=" + encodeURIComponent(c)), this.editorUi.editor.graph.addExtFont(b, c));
                            d = !0;
                            for (var e = 0; e < this.customFonts.length; e++)
                                if (this.customFonts[e].name == b) {
                                    d = !1;
                                    break
                                }
                            d && (this.customFonts.push({
                                name: b,
                                url: c
                            }), this.editorUi.fireEvent(new mxEventObject("customFontsChanged", "customFonts", this.customFonts)))
                        } finally {
                            a.getModel().endUpdate()
                        }
                    }
                }));
                this.editorUi.showDialog(c.container, 380, 250, !0, !0);
                c.init()
            }), c, null, !0)
        })))
    }
})();

EditorUi.prototype.cellProperties = {
    id: !0,
    value: !0,
    xmlValue: !0,
    vertex: !0,
    edge: !0,
    visible: !0,
    collapsed: !0,
    connectable: !0,
    parent: !0,
    children: !0,
    previous: !0,
    source: !0,
    target: !0,
    edges: !0,
    geometry: !0,
    style: !0,
    mxObjectId: !0,
    mxTransient: !0,
    seeAlso: !0
};
DiagramFormatPanel.prototype.init = function() {
    var a = this.editorUi.editor.graph;
    a.isEnabled() && (
        this.container.appendChild(this.addStyleOps(this.createPanel())),
        this.container.appendChild(this.addView(this.createPanel()))
        // this.container.appendChild(this.addPaperSize(this.createPanel())),
        // this.container.appendChild(this.addOptions(this.createPanel()))
    )
};
DiagramFormatPanel.prototype.addView = function(a) {
    var c = this.editorUi,
        d = c.editor.graph;
    a.appendChild(this.createTitle(mxResources.get("view")));
    this.addGridOption(a);
    DiagramFormatPanel.showPageView && a.appendChild(this.createOption(mxResources.get("pageView"), function() {
        return d.pageVisible
    }, function(a) {
        c.actions.get("pageView").funct()
    }, {
        install: function(a) {
            this.listener = function() {
                a(d.pageVisible)
            };
            c.addListener("pageViewChanged", this.listener)
        },
        destroy: function() {
            c.removeListener(this.listener)
        }
    }));

    return a
};
DiagramFormatPanel.prototype.addOptions = function(a) {
    var c = this.editorUi,
        d = c.editor.graph;
    a.appendChild(this.createTitle(mxResources.get("options")));
    d.isEnabled() && (a.appendChild(this.createOption(mxResources.get("connectionArrows"), function() {
        return d.connectionArrowsEnabled
    }, function(a) {
        c.actions.get("connectionArrows").funct()
    }, {
        install: function(a) {
            this.listener = function() {
                a(d.connectionArrowsEnabled)
            };
            c.addListener("connectionArrowsChanged", this.listener)
        },
        destroy: function() {
            c.removeListener(this.listener)
        }
    })), a.appendChild(this.createOption(mxResources.get("connectionPoints"), function() {
        return d.connectionHandler.isEnabled()
    }, function(a) {
        c.actions.get("connectionPoints").funct()
    }, {
        install: function(a) {
            this.listener = function() {
                a(d.connectionHandler.isEnabled())
            };
            c.addListener("connectionPointsChanged", this.listener)
        },
        destroy: function() {
            c.removeListener(this.listener)
        }
    })), a.appendChild(this.createOption(mxResources.get("guides"), function() {
        return d.graphHandler.guidesEnabled
    }, function(a) {
        c.actions.get("guides").funct()
    }, {
        install: function(a) {
            this.listener = function() {
                a(d.graphHandler.guidesEnabled)
            };
            c.addListener("guidesEnabledChanged", this.listener)
        },
        destroy: function() {
            c.removeListener(this.listener)
        }
    })));
    return a
};


DiagramFormatPanel.prototype.addGridOption = function(a) {
    function c(a) {
        var b = d.isFloatUnit() ? parseFloat(e.value) : parseInt(e.value),
            b = d.fromUnit(Math.max(d.inUnit(1), isNaN(b) ? d.inUnit(10) : b));
        b != f.getGridSize() && f.setGridSize(b);
        e.value = d.inUnit(b) + " " + d.getUnit();
        mxEvent.consume(a)
    }
    var d = this,
        b = this.editorUi,
        f = b.editor.graph,
        e = document.createElement("input");
    e.style.position = "absolute";
    e.style.textAlign = "right";
    e.style.width = "38px";
    e.value = this.inUnit(f.getGridSize()) + " " + this.getUnit();
    var h = this.createStepper(e, c, this.getUnitStep(), null, null, null, this.isFloatUnit());
    e.style.display = f.isGridEnabled() ? "" : "none";
    h.style.display = e.style.display;
    mxEvent.addListener(e, "keydown", function(a) {
        13 == a.keyCode ? (f.container.focus(), mxEvent.consume(a)) : 27 == a.keyCode && (e.value = f.getGridSize(), f.container.focus(), mxEvent.consume(a))
    });
    mxEvent.addListener(e, "blur", c);
    mxEvent.addListener(e, "change", c);
    var g = function(a, b) {
        e.value = d.inUnit(f.getGridSize()) + " " + d.getUnit();
        d.format.refresh()
    };
    f.view.addListener("unitChanged", g);
    this.listeners.push({
        destroy: function() {
            f.view.removeListener(g)
        }
    });
    if (mxClient.IS_SVG) {
        e.style.marginTop = "-2px";
        e.style.right = "84px";
        h.style.marginTop = "-16px";
        h.style.right = "72px";
        var k = this.createColorOption(mxResources.get("grid"), function() {
            var a = f.view.gridColor;
            return f.isGridEnabled() ? a : null
        }, function(a) {
            var c = f.isGridEnabled();
            a == mxConstants.NONE ? f.setGridEnabled(!1) : (f.setGridEnabled(!0), b.setGridColor(a));
            e.style.display = f.isGridEnabled() ? "" : "none";
            h.style.display = e.style.display;
            c != f.isGridEnabled() && b.fireEvent(new mxEventObject("gridEnabledChanged"))
        }, "#e0e0e0", {
            install: function(a) {
                this.listener = function() {
                    a(f.isGridEnabled() ? f.view.gridColor : null)
                };
                b.addListener("gridColorChanged", this.listener);
                b.addListener("gridEnabledChanged", this.listener)
            },
            destroy: function() {
                b.removeListener(this.listener)
            }
        });
        k.appendChild(e);
        k.appendChild(h);
        a.appendChild(k)
    } else e.style.marginTop = "2px", e.style.right = "32px", h.style.marginTop = "2px", h.style.right = "20px", a.appendChild(e), a.appendChild(h),
        a.appendChild(this.createOption(mxResources.get("grid"), function() {
            return f.isGridEnabled()
        }, function(a) {
            f.setGridEnabled(a);
            f.isGridEnabled() && (f.view.gridColor = "#e0e0e0");
            b.fireEvent(new mxEventObject("gridEnabledChanged"))
        }, {
            install: function(a) {
                this.listener = function() {
                    e.style.display = f.isGridEnabled() ? "" : "none";
                    h.style.display = e.style.display;
                    a(f.isGridEnabled())
                };
                b.addListener("gridEnabledChanged", this.listener)
            },
            destroy: function() {
                b.removeListener(this.listener)
            }
        }))
};
DiagramFormatPanel.prototype.addDocumentProperties = function(a) {
    a.appendChild(this.createTitle(mxResources.get("options")));
    return a
};
DiagramFormatPanel.prototype.addPaperSize = function(a) {
    var c = this.editorUi,
        d = c.editor.graph;
    a.appendChild(this.createTitle(mxResources.get("paperSize")));
    var b = PageSetupDialog.addPageFormatPanel(a, "formatpanel", d.pageFormat, function(a) {
        if (null == d.pageFormat || d.pageFormat.width != a.width || d.pageFormat.height != a.height) a = new ChangePageSetup(c, null, null, a), a.ignoreColor = !0, a.ignoreImage = !0, d.model.execute(a)
    });
    this.addKeyHandler(b.widthInput, function() {
        b.set(d.pageFormat)
    });
    this.addKeyHandler(b.heightInput, function() {
        b.set(d.pageFormat)
    });
    var f = function() {
        b.set(d.pageFormat)
    };
    c.addListener("pageFormatChanged", f);
    this.listeners.push({
        destroy: function() {
            c.removeListener(f)
        }
    });
    d.getModel().addListener(mxEvent.CHANGE, f);
    this.listeners.push({
        destroy: function() {
            d.getModel().removeListener(f)
        }
    });
    return a
};
DiagramFormatPanel.prototype.addStyleOps = function(a) {
    a.appendChild(this.createTitle("EP Info"));
    a.style.fontSize = "14px";
    root = this.editorUi.editor.graph.getModel().root;
    root_style = setStyleArray(root);
    if (root_style != null) {
        if ("URI" in root_style) {
            URI_val = root_style["URI"];
        } else {
            URI_val = "";
        }
        if ("prefix" in root_style) {
            prefix_val = root_style["prefix"];
        } else {
            prefix_val = "";
        }
        if ("title" in root_style) {
            title_val = root_style["title"];
        } else {
            title_val = "";
        }
        if ("creator" in root_style) {
            creator_val = root_style["creator"];
        } else {
            creator_val = "";
        }
        if ("description" in root_style) {
            description_val = root_style["description"];
        } else {
            description_val = "";
        }
        if ("identifier" in root_style) {
            identifier_val = root_style["identifier"];
        } else {
            identifier_val = "";
        }
    } else {
        URI_val = "";
        prefix_val = "";
        title_val = "";
        creator_val = "";
        description_val = "";
        identifier_val = "";
    }

    //Edit URI
    EP_URI_div = document.createElement("div");
    if (URI_val != "") {
        mxUtils.setTextContent(EP_URI_div, "URI : " + URI_val);
    } else {
        mxUtils.setTextContent(EP_URI_div, "URI : none");
    }
    EP_URI_div.style.width = "200px";
    EP_URI_div.style.overflow = "scroll";
    EP_URI_div.style.whiteSpace = "normal";
    EP_URI_div.className = "ep_uri_div";
    EP_URI_div.style.display = "table";
    EP_URI_div.style.marginBottom = "4px";
    EP_URI_div.style.wordBreak = "break-all";


    var c = mxUtils.button(mxResources.get("editURI"), mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("editURI").funct()
    }));

    c.setAttribute("title", mxResources.get("editURI") + " (" + this.editorUi.actions.get("editURI").shortcut + ")");

    function setButtonStyle(c) {
        c.style.marginBottom = "10px";
        c.style.width = "200px";
        c.style.height = "25px";
        c.style.backgroundColor = "rgb(255 255 255)";
        c.style.borderColor = "rgb(37 37 37)";
        c.style.borderRadius = "3px";
        c.style.borderWidth = "thin";
        c.className = "editButton";
        return c;
    }
    c = setButtonStyle(c);

    a.appendChild(EP_URI_div);
    a.appendChild(c);

    mxUtils.br(a);



    //Edit prefix
    EP_URI_prefix_div = document.createElement("div");
    if (prefix_val != "") {
        mxUtils.setTextContent(EP_URI_prefix_div, "Prefix : " + prefix_val);
    } else {
        mxUtils.setTextContent(EP_URI_prefix_div, "Prefix : none");
    }
    EP_URI_prefix_div.style.width = "200px";
    EP_URI_prefix_div.style.whiteSpace = "normal";
    EP_URI_prefix_div.className = "diagram_div";
    EP_URI_prefix_div.style.display = "table";
    EP_URI_prefix_div.style.marginBottom = "4px";
    EP_URI_prefix_div.style.wordBreak = "break-all";

    var c = mxUtils.button("Edit prefix", mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("editprefix").funct()
    }));
    c.setAttribute("title", mxResources.get("editprefix") + " (" + this.editorUi.actions.get("editprefix").shortcut + ")");
    c = setButtonStyle(c);

    a.appendChild(EP_URI_prefix_div);
    a.appendChild(c);
    mxUtils.br(a);

    //Edit title
    EP_URI_title_div = document.createElement("div");
    if (title_val != "") {
        mxUtils.setTextContent(EP_URI_title_div, "Title : " + title_val);
    } else {
        mxUtils.setTextContent(EP_URI_title_div, "Title : none");
    }
    EP_URI_title_div.style.width = "200px";
    EP_URI_title_div.style.whiteSpace = "normal";
    EP_URI_title_div.className = "diagram_div";
    EP_URI_title_div.style.display = "table";
    EP_URI_title_div.style.marginBottom = "4px";
    EP_URI_title_div.style.wordBreak = "break-all";

    var c = mxUtils.button("Edit title", mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("edittitle").funct()
    }));
    c.setAttribute("title", mxResources.get("edittitle") + " (" + this.editorUi.actions.get("edittitle").shortcut + ")");
    c = setButtonStyle(c);

    a.appendChild(EP_URI_title_div);
    a.appendChild(c);
    mxUtils.br(a);

    EP_URI_creator_div = document.createElement("div");
    if (creator_val != "") {
        mxUtils.setTextContent(EP_URI_creator_div, "Creator : " + creator_val);
    } else {
        mxUtils.setTextContent(EP_URI_creator_div, "Creator : none");
    }
    EP_URI_creator_div.style.width = "200px";
    EP_URI_creator_div.style.whiteSpace = "normal";
    EP_URI_creator_div.className = "diagram_div";
    EP_URI_creator_div.style.display = "table";
    EP_URI_creator_div.style.marginBottom = "4px";
    EP_URI_creator_div.style.wordBreak = "break-all";

    var c = mxUtils.button("Edit creator", mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("editcreator").funct()
    }));
    c.setAttribute("title", mxResources.get("editcreator") + " (" + this.editorUi.actions.get("editcreator").shortcut + ")");
    c = setButtonStyle(c);

    a.appendChild(EP_URI_creator_div);
    a.appendChild(c);
    mxUtils.br(a);

    //Edit description
    EP_URI_description_div = document.createElement("div");
    if (description_val != "") {
        mxUtils.setTextContent(EP_URI_description_div, "Description : " + description_val);
    } else {
        mxUtils.setTextContent(EP_URI_description_div, "Description : none");
    }
    EP_URI_description_div.style.width = "200px";
    EP_URI_description_div.style.whiteSpace = "normal";
    EP_URI_description_div.className = "diagram_div";
    EP_URI_description_div.style.display = "table";
    EP_URI_description_div.style.marginBottom = "4px";
    EP_URI_description_div.style.wordBreak = "break-all";

    var c = mxUtils.button("Edit description", mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("editdescription").funct()
    }));
    c.setAttribute("title", mxResources.get("editdescription") + " (" + this.editorUi.actions.get("editdescription").shortcut + ")");
    c = setButtonStyle(c);

    a.appendChild(EP_URI_description_div);
    a.appendChild(c);
    mxUtils.br(a);

    //Edit identifier
    EP_URI_identifier_div = document.createElement("div");
    if (identifier_val != "") {
        mxUtils.setTextContent(EP_URI_identifier_div, "Identifier : " + identifier_val);
    } else {
        mxUtils.setTextContent(EP_URI_identifier_div, "Identifier : none");
    }
    EP_URI_identifier_div.style.width = "200px";
    EP_URI_identifier_div.style.whiteSpace = "normal";
    EP_URI_identifier_div.className = "diagram_div";
    EP_URI_identifier_div.style.display = "table";
    EP_URI_identifier_div.style.marginBottom = "4px";
    EP_URI_identifier_div.style.wordBreak = "break-all";

    var c = mxUtils.button("Edit identifier", mxUtils.bind(this, function(a) {
        this.editorUi.actions.get("editidentifier").funct()
    }));
    c.setAttribute("title", mxResources.get("editidentifier") + " (" + this.editorUi.actions.get("editidentifier").shortcut + ")");
    c = setButtonStyle(c);

    a.appendChild(EP_URI_identifier_div);
    a.appendChild(c);
    mxUtils.br(a);
    return a
};

function setStyleArray(mxCell) {
    if (mxCell.style != undefined) {
        var style = mxCell.style;
        var style = style.split(';');
        for (var i = 0; i < style.length; i++) {
            style[i] = style[i].split('=');
        }
        var style_return = [];
        for (var i = 0; i < style.length; i++) {
            style_return[style[i][0]] = style[i][1]
        }
    } else {
        style_return = null
    }
    return style_return;
}

function setStyleString(styleArray) {
    var style_return = "";
    if (styleArray == null) {
        style_return = null;
    } else {
        for (let key in styleArray) {
            if (key == "" || key == undefined || styleArray[key] == "" || styleArray[key] == undefined) {
                continue;
            } else {
                style_return += key + "=" + styleArray[key] + ";";
            }
        }
    }

    console.log(style_return);
    return style_return;
}

Editor.prototype.getGraphXml = function(a) {
    a = (null != a ? a : 1) ? (new mxCodec(mxUtils.createXmlDocument())).encode(this.graph.getModel()) : this.graph.encodeCells(mxUtils.sortCells(this.graph.model.getTopmostCells(this.graph.getSelectionCells())));
    if (0 != this.graph.view.translate.x || 0 != this.graph.view.translate.y)
        a.setAttribute("dx", Math.round(100 * this.graph.view.translate.x) / 100),
        a.setAttribute("dy", Math.round(100 * this.graph.view.translate.y) / 100);
    a.setAttribute("grid", this.graph.isGridEnabled() ? "1" : "0");
    a.setAttribute("gridSize", this.graph.gridSize);
    a.setAttribute("guides", this.graph.graphHandler.guidesEnabled ? "1" : "0");
    a.setAttribute("tooltips", this.graph.tooltipHandler.isEnabled() ? "1" : "0");
    a.setAttribute("connect", this.graph.connectionHandler.isEnabled() ? "1" : "0");
    a.setAttribute("arrows", this.graph.connectionArrowsEnabled ? "1" : "0");
    a.setAttribute("fold", this.graph.foldingEnabled ? "1" : "0");
    a.setAttribute("page", this.graph.pageVisible ? "1" : "0");
    a.setAttribute("pageScale", this.graph.pageScale);
    a.setAttribute("pageWidth", this.graph.pageFormat.width);
    a.setAttribute("pageHeight", this.graph.pageFormat.height);
    null != this.graph.background && a.setAttribute("background", this.graph.background);
    return a
};

Graph.prototype.createViewState = function(a) {
    var e = a.getAttribute("page"),
        c = parseFloat(a.getAttribute("pageScale")),
        b = parseFloat(a.getAttribute("pageWidth")),
        k = parseFloat(a.getAttribute("pageHeight")),
        f = a.getAttribute("background"),
        l = a.getAttribute("backgroundImage"),
        l = null != l && 0 < l.length ? JSON.parse(l) : null,
        d = a.getAttribute("extFonts");
    if (d) try {
        d = d.split("|").map(function(a) {
            a = a.split("^");
            return {
                name: a[0],
                url: a[1]
            }
        })
    } catch (g) {
        console.log("ExtFonts format error: " + g.message)
    }
    return {
        gridEnabled: "0" != a.getAttribute("grid"),
        gridSize: parseFloat(a.getAttribute("gridSize")) || mxGraph.prototype.gridSize,
        guidesEnabled: "0" != a.getAttribute("guides"),
        foldingEnabled: "0" != a.getAttribute("fold"),
        shadowVisible: "1" == a.getAttribute("shadow"),
        pageVisible: this.isLightboxView() ? !1 : null != e ? "0" != e : this.defaultPageVisible,
        background: null != f && 0 < f.length ? f : null,
        backgroundImage: null != l ? new mxImage(l.src, l.width, l.height) : null,
        pageScale: isNaN(c) ? mxGraph.prototype.pageScale : c,
        pageFormat: isNaN(b) || isNaN(k) ? "undefined" === typeof mxSettings ? mxGraph.prototype.pageFormat : mxSettings.getPageFormat() : new mxRectangle(0, 0, b, k),
        tooltips: "0" != a.getAttribute("tooltips"),
        connect: "0" != a.getAttribute("connect"),
        arrows: "0" != a.getAttribute("arrows"),
        mathEnabled: "1" == a.getAttribute("math"),
        selectionCells: null,
        defaultParent: null,
        scrollbars: this.defaultScrollbars,
        scale: 1,
        extFonts: d || []
    }
};

StyleFormatPanel.prototype.addObjectInfo = function(a) {
    id_div = document.createElement("div");
    var cells = this.editorUi.editor.graph.getSelectionCells();
    mxUtils.writeln(id_div, "id : ");
    id_div.style.width = "200px";
    id_div.className = "id_div";
    id_div.style.marginBottom = "4px";
    id_div.style.whiteSpace = "normal";
    id_div.style.wordBreak = "break-all";
    id_div.style.height = "18px";

    textbox1 = document.createElement("input");

    root = this.editorUi.editor.graph.getModel().root;
    root_style = setStyleArray(root);

    if (root_style != null) {
        // if("URI" in root_style){
        //   URI_val = root_style["URI"];
        // }else{
        //   URI_val = "";
        // }
        if ("prefix" in root_style) {
            prefix_val = root_style["prefix"];
        } else {
            prefix_val = "";
        }
    } else {
        // URI_val = "";
        prefix_val = "";
    }

    if (prefix_val == "") {
        str = "";
    } else {
        str = "[" + prefix_val + "]";
    }

    textbox1.value = str + cells[0].id;
    textbox1.type = "text";
    textbox1.style.width = "200px";
    textbox1.className = "textbox";
    textbox1.style.marginBottom = "4px";
    textbox1.style.whiteSpace = "normal";
    textbox1.style.wordBreak = "break-all";
    textbox1.style.borderColor = "rgb(37 37 37)";
    textbox1.style.borderRadius = "2px";
    textbox1.style.borderWidth = "thin";

    a.appendChild(id_div);
    a.appendChild(textbox1);

    textbox2 = document.createElement("input");
    // mxUtils.setValue(textbox2, cells[0].id);
    // textbox2.value = cells[0].id;
    // textbox2.type="text";
    // textbox2.style.width = "200px";
    // textbox2.className= "textbox";
    // textbox2.style.marginBottom="2px";
    // textbox2.style.whiteSpace="normal";
    // textbox2.style.wordBreak="break-all";
    // textbox2.style.borderColor="rgb(37 37 37)";
    // textbox2.style.borderRadius="2px";
    // textbox2.style.borderWidth="thin";

    seeAlso_div = document.createElement("div");
    var cells = this.editorUi.editor.graph.getSelectionCells();
    var cell_style = setStyleArray(cells[0]);
    var seeAlso_val = "";

    if (cell_style != null) {
        if ("seeAlso" in cell_style) {
            seeAlso_val = cell_style["seeAlso"];
        } else {
            seeAlso_val = "";
        }
    } else {
        seeAlso_val = "";
    }

    if (seeAlso_val != "") {
        mxUtils.writeln(seeAlso_div, "seeAlso : ");
        mxUtils.write(seeAlso_div, seeAlso_val);
    } else {
        mxUtils.write(seeAlso_div, "seeAlso : none");
    }
    // mxUtils.writeln(seeAlso_div, "seeAlso : ");
    // if (cells[0].seeAlso != undefined){
    //   textbox2.value = cells[0].seeAlso;
    // }else{
    //   textbox2.value = "";
    // }
    seeAlso_div.style.width = "200px";
    seeAlso_div.className = "seeAlso_div";
    seeAlso_div.style.whiteSpace = "normal";
    seeAlso_div.style.wordBreak = "break-all";
    seeAlso_div.style.marginBottom = "4px";

    a.appendChild(seeAlso_div);
    // a.appendChild(textbox2);

    return a;

};

q = StyleFormatPanel.prototype.addWEBAPI;
StyleFormatPanel.prototype.addWEBAPI = function(a) {
    var d = mxUtils.button("Get EP Description", mxUtils.bind(this, function(a) {
        // var getval = mxUtils.load("http://digital-triplet.net:5000/ep/expertB/actions");
        // mxUtils.popup(getval);
        // mxUtils.get("http://digital-triplet.net:5000/ep/expertB/actions", function(){
        //   mxUtils.popup();
        // });
        console.log(mxUtils.get("http://digital-triplet.net:5000/ep/expertB/actions"));
        mxUtils.popup(mxUtils.get("http://digital-triplet.net:5000/ep/expertB/actions"));
    }));
    d.setAttribute("title", mxResources.get("copyStyle") + " (" + this.editorUi.actions.get("copyStyle").shortcut + ")");
    d.style.marginBottom = "2px";
    d.style.width = "202px";
    d.style.marginRight = "2px";
    d.style.height = "20px";
    d.style.backgroundColor = "rgb(242 242 242)";
    d.style.borderColor = "rgb(37 37 37)";
    d.style.borderRadius = "3px";
    d.style.borderWidth = "thin";
    a.appendChild(d);
    return a;
};

StyleFormatPanel.prototype.init = function() {
    var a = this.format.getSelectionState();

    a = this.addObjectInfo(this.createPanel());
    // a = this.addStyleOps(this.createPanel());
    this.container.appendChild(this.addStyleOps(a));

    null != a.firstChild && mxUtils.br(a);
    this.container.appendChild(this.addEditOps(a));

    // null != a.firstChild && mxUtils.br(a);
    // this.container.appendChild(this.addWEBAPI(a));

    // null != a.firstChild && mxUtils.br(a);
    // this.container.appendChild(this.addStyleOps(a));
    // this.container.appendChild(this.addObjectInfo(a));


    var a = this.format.getSelectionState();

    a.containsLabel || (
        a.containsImage && 1 == a.vertices.length &&
        "image" == a.style.shape && null != a.style.image && "data:image/svg+xml;" == a.style.image.substring(0, 19) &&
        this.container.appendChild(this.addSvgStyles(this.createPanel())),
        a.containsImage && "image" != a.style.shape ||
        this.container.appendChild(this.addFill(this.createPanel())),
        this.container.appendChild(this.addStroke(this.createPanel())),
        this.container.appendChild(this.addLineJumps(this.createPanel()))
        // , a = this.createRelativeOption(mxResources.get("opacity"), mxConstants.STYLE_OPACITY, 41), a.style.paddingTop = "8px", a.style.paddingBottom = "8px", 
        // this.container.appendChild(a) 
        // this.container.appendChild(this.addEffects(this.createPanel()))
    );
    // a = this.addEditOps(this.createPanel());
    // null != a.firstChild && mxUtils.br(a);
    // this.container.appendChild(this.addStyleOps(a));
};