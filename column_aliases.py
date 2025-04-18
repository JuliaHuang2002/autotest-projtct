# column_aliases.py

# 字段别名字典：中文名 → 英文字段名
COLUMN_ALIASES = {
    '数据时间': 'date_time',
    '日期': 'date_time',
    '时间类型：日、周、月、季、年': 'date_type',
    '区域名称': 'area_name',
    '城市名称': 'city_name',
    '城市公司': 'company_name',
    '项目名称': 'project_name',
    '分期名称': 'stage_name',
    '所处核心城市': 'main_city',
    '城市级别': 'city_level',
    '城市群组': 'qz_number',
    '拿地年份': 'nd_time',
    '土地名称简称': 'td_name',
    '是否置地项目': 'if_zd_pro',
    '项目法人公司': 'pro_fr_company',
    '业态名称': 'first_format',
    '产品类型名称': 'format_name',
    '产品名称': 'product_name',
    '操盘类型': 'cp_type',
    '并表类型': 'bb_type',
    '项目属性': 'project_nature',
    '主控设计': 'zk_sj',
    '主控营销': 'zk_yx',
    '主控采购': 'zk_cg',
    '主控工程': 'zk_gc',
    '主控成本': 'zk_cb',
    '主控财务': 'zk_cw',
    '主控客服': 'zk_kf',
    '主控物业': 'zk_wy',
    '数据来源': 'data_source',
    '推盘类型': 'tp_type',
    '住宅项目分类': 'project_product_type',
    '是否重点项目': 'sf_main',
    '是否来访非持销': 'sf_lfcx',
    '权益比例': 'equity_rate',
    '净上划金额': 'jshje',
    '认购金额': 'rgje',
    '权益认购金额': 'qyrgje',
    '净认购金额': 'jrgje',
    '权益净认购金额': 'qyjrgje',
    '装修认购金额': 'zxrgje',
    '装修净认购金额': 'zxjrgje',
    '认购套数': 'rgts',
    '净认购套数': 'jrgts',
    '认购面积': 'rgmj',
    '权益认购面积': 'qyrgmj',
    '净认购面积': 'jrgmj',
    '权益净认购面积': 'qyjrgmj',
    '认购退房金额': 'rgtfje',
    '权益认购退房金额': 'qyrgtfje',
    '认购退房套数': 'rgtfts',
    '认购退房面积': 'rgtfmj',
    '权益认购退房面积': 'qyrgtfmj',
    '认购挞定金额': 'rgtdje',
    '权益认购挞定金额': 'qyrgtdje',
    '认购挞定套数': 'rgtdts',
    '认购挞定面积': 'rgtdmj',
    '权益认购挞定面积': 'qyrgtdmj',
    '认购未签约套数': 'rwqts',
    '认购未签约面积': 'rwqmj',
    '权益认购未签约面积': 'qyrwqmj',
    '认购未签约金额': 'rwqje',
    '权益认购未签约金额': 'qyrwqje',
    '签约金额': 'qyje',
    '权益签约金额': 'qyqyje',
    '签约套数': 'qyts',
    '签约面积': 'qymj',
    '权益签约面积': 'qyqymj',
    '签约退房金额': 'qyrfje',
    '签约退房套数': 'qytfts',
    '权益签约退房金额': 'qyqytfje',
    '签约退房面积': 'qytfmj',
    '权益签约退房面积': 'qyqytfmj',
    '净签约金额': 'jqyje',
    '权益净签约金额': 'qyjqyje',
    '净签约套数': 'jqyts',
    '净签约面积': 'jqymj',
    '权益净签约面积': 'qyjqymj',
    '草签套数': 'cqts',
    '草签金额': 'cqje',
    '权益草签金额': 'qycqje',
    '装修净签约金额': 'zxjqyje',
    '权益装修净签约金额': 'qyzxjqyje',
    '签约补差面积': 'qybcmj',
    '回款': 'hk',
    '权益回款': 'qyhk',
    '签约回款': 'qy_hk',
    '入湖时间戳': 'etl_in_dt',
    '数据更新时间': 'update_time'
}


# 英文指标字段（技术名称）
metric_fields_en = [
    'equity_rate', 'jshje', 'rgje', 'qyrgje', 'jrgje', 'qyjrgje',
    'zxrgje', 'zxjrgje', 'rgts', 'jrgts', 'rgmj', 'qyrgmj', 'jrgmj',
    'qyjrgmj', 'rgtfje', 'qyrgtfje', 'rgtfts', 'rgtfmj', 'qyrgtfmj',
    'rgtdje', 'qyrgtdje', 'rgtdts', 'rgtdmj', 'qyrgtdmj', 'rwqts',
    'rwqmj', 'qyrwqmj', 'rwqje', 'qyrwqje', 'qyje', 'qyqyje', 'qyts',
    'qymj', 'qyqymj', 'qyrfje', 'qytfts', 'qyqytfje', 'qytfmj',
    'qyqytfmj', 'jqyje', 'qyjqyje', 'jqyts', 'jqymj', 'qyjqymj',
    'cqts', 'cqje', 'qycqje', 'zxjqyje', 'qyzxjqyje', 'qybcmj',
    'hk', 'qyhk', 'qy_hk'
]
# 中文指标字段（业务含义）
metric_fields_cn = [
    '权益比例', '净上划金额', '认购金额', '权益认购金额', '净认购金额', '权益净认购金额',
    '装修认购金额', '装修净认购金额', '认购套数(不含车位和配套)', '认购套数', '净认购套数(不含车位和配套)',
    '认购面积', '权益认购面积', '净认购面积', '权益净认购面积', '认购退房金额',
    '权益认购退房金额', '认购退房套数(不含车位和配套)', '认购退房套数', '认购退房面积', '权益认购退房面积',
    '认购挞定金额', '权益认购挞定金额', '认购挞定套数(不含车位和配套)', '认购挞定面积',
    '权益认购挞定面积', '认购未签约套数(不含车位和配套)', '认购未签约面积', '权益认购未签约面积',
    '认购未签约金额', '权益认购未签约金额', '签约金额', '权益签约金额',
    '签约套数(不含车位和配套)', '签约面积', '权益签约面积', '签约退房金额',
    '签约退房套数(不含车位和配套)', '权益签约退房金额', '签约退房面积', '权益签约退房面积',
    '净签约金额', '权益净签约金额', '净签约套数(不含车位和配套)', '净签约面积',
    '权益净签约面积', '草签套数(不含车位和配套)', '草签金额', '权益草签金额',
    '装修净签约金额', '权益装修净签约金额', '签约补差面积', '回款', '权益回款', '签约回款'
]

METRIC_FIELDS = set(metric_fields_en) | set(metric_fields_cn)