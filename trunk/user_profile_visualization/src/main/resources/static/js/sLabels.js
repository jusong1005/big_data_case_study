Vue.component('select-label', {
    props:['showDialog','_componentData'],
    template: '#s_label',
    data(){
        return {
            activeName:'0',
            label_level_1:null,
            leafLabels:null,
            disabled: false,
            dialogFormVisible:false,
            data:this._componentData,
            ids:{
                id:[],
                cusId:0
            },
            currentPage: 1,
            pageSize: 30,
            total: 0,
            search:{
                id:0,
                level:3
            },
            labelId:0//当前选中的一级标签的id
        }
    },
    watch:{
        _componentData:{  //监听父组件中componentData属性的变化（只有id改变）
               handler(newValue, oldValue) {
                   this.data = newValue;
                   this.ids.cusId = this.data.id;//实时获取画像中中客户的id
               },
               deep: true
        }
    },
    created(){
        this.loadData();
        console.log(this._componentData);
    },
    methods:{
        open(){
            this.dialogFormVisible=true;
        },
        closeDialog(){
            this.dialogFormVisible = false;
            this.ids.id = [];
        },
        loadData(){
            var _self = this;
            $.ajax({
                url:_self.data.url_1,
                method: "get",
                data:_self.search,
                contentType: "application/json;charset=UTF-8",
                success: function (data) {
                    _self.label_level_1 = data.labels;
                    if(data.labels.length>0){
                        _self.activeName = data.labels[0].name;
                        _self.loadLeafLabels(data.labels[0].id);
                        _self.labelId = data.labels[0].id;
                    }
                },
                error: function () {
                }
            });
        },
        resetChecked(){
            this.ids.id=[];
            this.dialogFormVisible = false;
        },
        onSubmit(){
            var _self = this;
            var label = JSON.stringify(_self.ids);
            this.disabled = true;
            jQuery.ajax({
                url:_self.data.url_3,
                method: 'post',
                data: label,
                contentType: "application/json;charset=UTF-8",
                success: function (data) {
                    if (data.status == "SUCCESS") {
                        _self.$notify({
                            message: data.message,
                            type: 'success',
                            duration: 1000,//1秒后关闭
                            onClose: function () {
                                _self.disabled = false;
                            }
                        });
                        _self.resetChecked();
                        _self.$emit('fresh');//触发父组件loadCusProData方法，重新获取客户标签

                    } else {
                        _self.disabled = false;
                        _self.$notify({
                            message: data.message,
                            type: 'error'
                        });
                    }
                },
                error: function (data) {}
            });


        },
        handleClick(tab, event) {
            var that = this;
            that.labelId = tab.$attrs.value;
            that.loadLeafLabels(that.labelId);
            that.currentPage =  1;
            that.pageSize = 30;

        },
        loadLeafLabels(id){
            var _self = this;
            _self.search.id = id;
            _self.search.pageSize = this.pageSize;
            _self.search.pageNumber = this.currentPage;
            $.ajax({
                url: _self.data.url_2,
                data:_self.search,
                method: "get",
                contentType: "application/json;charset=UTF-8",
                success: function (data) {
                    _self.leafLabels = data.content;
                    _self.total = data.totalElements;
                },
                error: function () {

                }
            });
        },
        //分页大小修改事件
        pageSizeChange: function (val) {
            this.pageSize = val;
            this.loadLeafLabels(this.labelId);//重新加载数据
        },
        //当前页修改事件
        currentPageChange: function (val) {
            this.currentPage = val;
            this.loadLeafLabels(this.labelId);//重新加载数据
        },
    }
});