from enum import Enum

class MetabaseDatabase(Enum):
    #Codigo referente ao database no metabase
    ClickhousePartnerZeroum = 67
    ClickhousePartnerEnergiabet = 103

class MetabaseTable(Enum):
    #Codigo referente a consulta no metabase
    ZeroUm_AffiliatePlatform = 2514
    ZeroUm_AffiliateReferral = 2513
    ZeroUm_Bet = 1041
    ZeroUm_BetOptimised = 1785
    ZeroUm_Bonus = 1062
    ZeroUm_BonusProduct = 1076
    ZeroUm_Client = 1043
    ZeroUm_ClientBonus = 1052
    ZeroUm_ClientDailyBalance = 1081
    ZeroUm_ClientSession = 1055
    ZeroUm_GameProvider = 1039
    ZeroUm_PaymentRequest = 1038
    ZeroUm_PaymentSystem = 2575
    ZeroUm_Product = 1047
    ZeroUm_ProductCategory = 1061
    ZeroUm_Region = 2515
    ZeroUm_SportsbookBet = 1065
    ZeroUm_SportsbookBetSelection = 1048
    ZeroUm_SportsbookCompetition = 1070
    ZeroUm_SportsbookMarket = 1046
    ZeroUm_SportsbookMarketType = 1057
    ZeroUm_SportsbookMatch = 1080
    ZeroUm_SportsbookMatchBet = 1594
    ZeroUm_SportsbookPlayer = 1078
    ZeroUm_SportsbookRegion = 1083
    ZeroUm_SportsbookSelection = 1074
    ZeroUm_SportsbookSelectionSetting = 1882
    ZeroUm_SportsbookSelectionType = 1042
    ZeroUm_SportsbookSport = 1036
    ZeroUm_SportsbookTeam = 1049

    EnergiaBet_Account = 2743
    EnergiaBet_AccountType = 2744
    EnergiaBet_AffiliatePlatform = 2512
    EnergiaBet_AffiliateReferral = 2511
    EnergiaBet_Bet = 1290
    EnergiaBet_Bonus = 1309
    EnergiaBet_BonusProduct = 1325
    EnergiaBet_Client = 1298
    EnergiaBet_ClientBonus = 1311
    EnergiaBet_ClientDailyBalance = 1333
    EnergiaBet_ClientFulltable = 3572
    EnergiaBet_ClientSession = 1317
    EnergiaBet_Correction = 3443
    EnergiaBet_DatesOfYear = 1855
    EnergiaBet_Document = 3407
    EnergiaBet_Feature = 3495
    EnergiaBet_GameProvider = 1287
    EnergiaBet_MvUnifiedDailyRevenue = 3372
    EnergiaBet_ObjectType = 2679
    EnergiaBet_PaymentRequest = 1302
    EnergiaBet_PaymentRequestHistory = 2822
    EnergiaBet_PaymentSystem = 2576
    EnergiaBet_Product = 1291
    EnergiaBet_ProductCategory = 1331
    EnergiaBet_ProductFeature = 3562
    EnergiaBet_ProductTagPartner = 3493
    EnergiaBet_ProductTagPartnerSetting = 3518
    EnergiaBet_ProductTheme = 3545
    EnergiaBet_Region = 2510
    EnergiaBet_SportsbookBet = 1319
    EnergiaBet_SportsbookBetSelection = 1307
    EnergiaBet_SportsbookBetSelectionLiability = 3177
    EnergiaBet_SportsbookCompetition = 1292
    EnergiaBet_SportsbookCompetitor = 1326
    EnergiaBet_SportsbookMarket = 1321
    EnergiaBet_SportsbookMarketType = 1318
    EnergiaBet_SportsbookMatch = 1288
    EnergiaBet_SportsbookMatchBet = 1595
    EnergiaBet_SportsbookPlayer = 1328
    EnergiaBet_SportsbookRegion = 1310
    EnergiaBet_SportsbookSelection = 1313
    EnergiaBet_SportsbookSelectionSetting = 1884
    EnergiaBet_SportsbookSelectionType = 1299
    EnergiaBet_SportsbookSport = 1324
    EnergiaBet_SportsbookTeam = 1305
    EnergiaBet_Theme = 3544
    EnergiaBet_TranslationEntry = 3496
    EnergiaBet_TriggerSetting = 3494



class MetabaseCard(Enum):
    #Codigo referente a consulta criada por nós no metabase
    ZeroUm_Stage = "card__10488"
    ZeroUm_Deposito = "card__11354"
    ZeroUm_PrimeiraAposta = "card__11387"
    ZeroUm_Saque = "card__11386"
    ZeroUm_UltimaAposta = "card__11388"
    ZeroUm_Bonus = "card__11419"
    ZeroUm_StageSport = "card__11430"
    ZeroUm_BonusAtivado = "card__13795"
    ZeroUm_DepositoSaqueDias = "card__14752"
    ZeroUm_Jogos = "card__14687"
    ZeroUm_ApostasJogosHora = "card__14785"
    ZeroUm_Usuarios = "card__14826"
    ZeroUm_UsuariosTotalizador = "card__14828"
    ZeroUm_UsuariosTotalizadorBet = "card__14917"

    #Codigo das consultas de validação
    ZeroUm_Validacao_ApostasDia = "card__17300"
    ZeroUm_Validacao_DepositoSaque = "card__17297"
    ZeroUm_Validacao_RegistroUsuario = "card__17299"


    EnergiaBet_Stage = "card__15848"
    EnergiaBet_Deposito = "card__15842"
    EnergiaBet_PrimeiraAposta = "card__15845"
    EnergiaBet_Saque = "card__15846"
    EnergiaBet_UltimaAposta = "card__15849"
    EnergiaBet_Bonus = "card__15809"
    EnergiaBet_StageSport = "card__15847"
    EnergiaBet_BonusAtivado = "card__15841"
    EnergiaBet_DepositoSaqueDias = "card__15843"
    EnergiaBet_Jogos = "card__15844"
    EnergiaBet_ApostasJogosHora = "card__15808"
    EnergiaBet_Usuarios = "card__15850"
    EnergiaBet_UsuariosTotalizador = "card__15851"
    EnergiaBet_UsuariosTotalizadorBet = "card__15852"


    #Codigo das consultas de validação
    EnergiaBet_Validacao_ApostasDia = "card__17513"
    EnergiaBet_Validacao_DepositoSaque = "card__17514"
    EnergiaBet_Validacao_RegistroUsuario = "card__17515"

    #Codigo das consultas de Saldo/Sessão (Account, ClientDailyBalance, ClientSession)
    ZeroUm_SaldoRealtime = "card__20397"
    ZeroUm_SaldoDiario = "card__20433"
    ZeroUm_SessoesDiarias = "card__20434"
 
    EnergiaBet_SaldoRealtime = "card__20396"
    EnergiaBet_SaldoDiario = "card__20432"
    EnergiaBet_SessoesDiarias = "card__20527"