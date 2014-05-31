#!/usr/bin/env python
#! -*- encoding: utf-8 -*-


# name:mapper.py
# date:2007-11-21 11:07:06.880064
#
# 
# Time-stamp: " "
#


import sys
import re

import MeCab

debug = False

stdin = sys.stdin
stdout = sys.stdout
stderr = sys.stderr


class MecabParser(object):


    def __init__(self, min_len=2, max_len=-1):
        try:
            self.tagger = MeCab.Tagger ()
        except:
            stderr.write('! failed to instantiate the tagger !')


        self.min_len = min_len
        self.max_len = max_len

        # skip word rule
        nw_pat = re.compile(r'[()|_\\t]+')


    def _reject(self, morph, features):
        """
        detect irrelevant term
        
        """
        if features[1] == '数':
            return True

        s = unicode(morph.surface.strip(), 'utf-8')
        l = len(s) 
        if min_len > 0 and l < min_len:
            return True

        if max_len > 0 and l > max_len:
            return True

        m = nw_pat.search(s)
        if m:
            return True
    
        return False


    def _accept(self, morph, features):
        """
        detecter relevant term
        
        """
        
        if features[0] == '名詞':
            return True
        
        #if features[0] == '動詞':
        #    return True
    

        return False

    def _eos(self, morph, fs):
        """
        simple eos detecter
        
        
        """
        if fs[1] == '句点' or morph.surface == '。':
            return True
        
        if morph.surface == '.' and fs[1] == 'サ変接続' and fs[0] == '名詞':
            return True
        
        return False
    

    def split(self, body):
        sentences = []
        try:
            m = self.tagger.parseToNode(body)
            
            sentence = []
            while m:
                features = m.feature.split(',')
                if self._eos(m, features):
                    sentences.append(sentence)
                    sentence = []

                srfc = m.surface
                sentence.append(srfc)
            
                m = m.next

            sentences.append(sentence)

    
        except RuntimeError,  e:
            stderr.write("RuntimeError: %s" %(str(e)))


        return sentences


    def parse(self, body):
        """
        parse via mecab
        
        
        """
        sentences = []
        try:
            m = self.tagger.parseToNode(body)
            
            sentence = []
            while m:
                features = m.feature.split(',')

                if self._eos(m, features):
                    sentences.append(sentence)
                    sentence = []

                if self._accept(m, features) and not _reject(m, features):
                    srfc = m.feature.split(',')[6]
                    if srfc == '*':
                        srfc = m.surface
                    sentence.append(srfc)

                    if debug:
                        stderr.write('suface:%s feature:%s\n' %(m.surface, m.feature))

                        
                m = m.next
        

            sentences.append(sentence)

    
        except RuntimeError,  e:
            stderr.write("RuntimeError: %s" %(str(e)))
            

        return sentences
        


if __name__ == '__main__':
    print 'mecabparser.py'

    text ="""

       FB_00001.txt
        議　題：JUST Suite Police 2010(L事業部2010/6/15発売予定)について
        
【議事録】商品化承認会議　JUST Suite Police 2010

────────────────────────────────────
日　時：2009年12月16日(水)　13:50-14:20
────────────────────────────────────
参加者：(東京)福良・植松・石沢・稲野
　　　　(徳島)出野・関灘・三木・三浦・仁科・高田・出張　　　　　※敬称略
────────────────────────────────────
議　題：JUST Suite Police 2010(L事業部2010/6/15発売予定)について
　　　　商品化承認会議を実施
────────────────────────────────────
資　料：別途送付のとおり
　　　　（注）送付した企画書(pdfファイル)はJL顧客情報の詳細が記載されて
　　　　　　　いるため、OMページには掲載しない予定です。ご了承ください。
────────────────────────────────────
■結論
　┌──┐
　│承認│
　└──┘
　　→2010年6月ライセンス発売に向けて商品化を進める
　　→課題：なし
　　→商品化決裁書：修正箇所なし

────────────────────────────────────
▼確認事項

■企画説明（ライセンス事業部企画部 高田克久さんより）
・収支について
　　売上目標：2010年度内 約1.2億円／1年間 約1.5億円
　　直接原価：約500万円弱
　　貢献利益：2010年度内 約1.2億円(利益率96％)／1年間 約1.5億円(97％)
　　→収支的には問題のない製品
　　→SuitePolice、太&花Police、花子Policeの3ラインナップで
　　　年度内1万本を目指す
　　→単価UPを基本に、3ヶ年で5億円を目指す

・Police商品の開発経緯
　　警察向けの営業活動を2005年以降注力したことで、
　　警察市場における花子の導入数が急速に伸びる（花子の法人導入の約9割）
　　→これによりPolice商品を市場投入
　　　顧客単価UPに加え、一太郎のロックインが狙い
　　→既存顧客に対しては、
　　　（太）　　→（太&花Police）
　　　（太＋花）→（SuitePolice2010）
　　　へと誘導する営業活動を行う

・現在の状況
　　ガバメント製品と同様に、2009年度はPCリプレイスの谷間にあたり、
　　花子の警察市場への導入数も2005〜2007年をピークに減少傾向
　　　→2009年度は11月末時点の売上が約1600万円（9月発売〜3ヶ月の実績）
　　MSの動向は要注意
　　　→Visioの強化により注視が必要（コンテンツ無償提供/専用ソフト開発）
　　　　MSも業界特化型商材の傾向あり
　　　　Visiono導入率を調査しておきたい

・開発ポイント
　　Suiteを2010世代へVUP
　　花子Policeも2010ベースに置き換え（花子のふりがな機能などを搭載）
　　イラスト増強
　　オンラインアップデート機能forJ-Licenseを搭載
　　公用文辞書やPDFソフトの内製化
　　ダウングレード利用不可（ガバと異なり旧Verの併売は不要）

・販促のための注力活動としては、
　　都道府県ごとの警察へ営業活動、警察新聞への広告展開など
　　また、可能であれば、花子Policeもコンテンツの無料DL提供を検討したい

■質疑
・これまでの営業活動について説明
　→警察で「花子」を使用する部署は限られていた(数十〜百人程度)が、
　　ライセンス営業で「警察＝花子」ということで注力して営業活動してみた
　→警察では、市民との交流などでイラストを多用するシーンも増えており、
　　太郎との併用、実況見分の作成などで、警察内の力の強い部署を切り口に
　　導入を広げられつつある状況（署内一括導入に結びつける活動をしている）
　→PRとしては、日刊警察新聞への広告展開などは非常に有効である

・Visioに対する評判
　→イラストが海外風なので使いづらい、らしく、
　　その点で花子は優位（複数の県警からのヒアリングを元に製品化）
　　ただし花子を署内一括導入という事例はまだまだ先進的であり稀である

・ターゲットに警察の外郭団体も加えることは可能か？
　→外郭団体としては「安全協会」が最大規模で、警察OBも多く反応は上々、
　　警察新聞広告などを元に、購入要望も実際に届いている
　→外郭団体も視野に入れた場合「Police」という名称はどうか？
　　警察の予算は県庁が握っており、一太郎排除の傾向を考えると
　　この名称の方が導入されやすい
　→警備会社や弁護士などもターゲットに想定し、
　　JL-Gov以外のメニュー（JL-St/Ex）も用意している
　→関連団体を狙うなら、ターゲットはかなり絞ることができるので、
　　販促のほうでリストアップし、効率的に提案を進めるようにしてください

・警察の予算の付き方に傾向はあるか？
　→予算期は県により時期がバラバラで傾向はない
　→これまでの実績では、大口案件が1〜2月に集中していることから
　　売上は第4Qが最大となっている

・MSの動向は？（警察関連にどのようにアプローチしているか）
　→警察市場に対しては、MSの営業が直接活動している
　　エクセルよりもMS Officeを値下げする形で提案している様子
　→MSの営業トークとしては「一太郎は税金の無駄遣い」と言っている様子
　　また、花子との親和性を謳いVisioを提案しているらしい

・コンテンツが有用とのことだがリプレイスのタイミングでの提供でよいのか？
　→旬素材をどのように提供していくのかは、今後要検討
　→また、各地方の特性を考慮したコンテンツの搭載も有用と考えている
　　（例：雪国向けに縦型信号機のコンテンツを準備する、など）

────────────────────────────────────
▼本会議での結論（福良社長より）
　本日の内容にて商品化を「承認」とします

／以上。
    """
    p = MecabParser()
    result = p.parse(text)

    for s in result:

        for m in s:
            print m

