Vue.component('label-search', {
    props:['showDialog','_labelData'],
    template: '#label_s',
    data(){
        return{
            selectedLabelVisible:false,
            loading: false,
            LabSearchForm:{
                parentLabelId:null,
                leafLabelName:'',
            },
            pageSize:10,
            currentPage:1,
            total:0,
            selectTotalLabels:this._labelData.labelId,//存放所有添加标签查询窗口中选中标签
            tempLabels:[],//临时存放单个标签下的子标签
            label_name: [],
            leafLabel:[],
            getRowKeys(row) {
                return row.id;
            },
        }
    },
    watch:{
        _labelData:{  //监听父组件中当前激活页的变化
            handler(newValue, oldValue) {
                if(newValue['activeTab'] === '标签名称'){
                    this.selectTotalLabels= this._labelData.labelId;
                }else{
                    this.selectTotalLabels = this._labelData.labelId_no;
                }
                if(newValue['flag']){
                    this.selectTotalLabels = [];
                    this.tempLabels = [];
                    this.LabSearchForm.leafLabelName = '';
                    this.LabSearchForm.parentLabelId = null;
                    this.getLeafLabels();

                }
            },
            deep:true
        },
        '_labelData.delLabel':{
            handler(newValue, oldValue) {
                var _self = this;
                var index =  _self.tempLabels.indexOf(newValue);
                if(index != -1){
                    _self.tempLabels.splice(index,1);
                }
            }
        }
    },
    created(){
        this.getLeafLabels();
    },
    methods:{
        pageSizeChange(val){//添加标签查询---分页大小改变事件
            this.pageSize = val;
            this.getLeafLabels();
        },
        currentPageChange(val){//添加标签查询--当前页改变事件
            this.currentPage = val;
            this.getLeafLabels();
        },
        pageSizeChange(val){//添加标签查询---分页大小改变事件
            this.pageSize = val;
            this.getLeafLabels();
        },
        getLeafLabels(){
            this.LabSearchForm.pageSize = this.pageSize;
            this.LabSearchForm.pageNumber = this.currentPage;
            var _self = this;
            $.ajax({
                url: 'label/customer/leaf/search',
                data:this.LabSearchForm,
                method: "get",
                data:this.LabSearchForm,
                contentType: "application/json;charset=UTF-8",
                success: function (data) {
                    _self.leafLabel = data.content;
                    _self.total = data.totalElements;
                },
                error: function () {
                }
            });
        },
        push2FinalLabels(){//添加选中的标签
            var _self = this;
            if(this.tempLabels.length>0){
                for(var i=0;i<this.tempLabels.length;i++){
                    if(_self.selectTotalLabels.indexOf(_self.tempLabels[i]) == -1){
                        _self.selectTotalLabels.push(_self.tempLabels[i]);
                    }
                }
                this.$notify({
                    title: '成功',
                    message: '添加成功',
                    type: 'success'
                });
            }else{
                this.$notify({
                    title: '提示',
                    message: '请先选择标签',
                    type: 'warning'
                });
            }

        },
        handleSelectionChange(val){
            this.tempLabels = val;
        },
        preViewSelectedLabels(){//查看已选择的标签
            this._labelData.visible = true;
            this.$emit('reload', this.selectTotalLabels);
        }
        ,
        remoteMethod(name) {
            var _self = this;
            if (name !== '') {
                _self.loading = true;
                $.ajax({
                    url:'label/customer/search/name/'+name,
                    method:'get',
                    contentType: "application/json;charset=UTF-8",
                    success: function (data) {
                        _self.list = data;
                        setTimeout(() => {
                            _self.loading = false;
                            _self.label_name = _self.list.filter(item => {
                                return true;
                            });

                        }, 200);
                    },
                    error: function () {}
                });
            } else {
                _self.label_name = [];
            }
        },
    }
});