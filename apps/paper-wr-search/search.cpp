// Offline search only. No network, signing, wallet or active configuration APIs.
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std;
struct Row { double t,px,lo,hi,age,bfage,gap,fold; };
struct Run { int32_t start,end,d; };
struct Tree {
    int n; bool maximum; vector<double> a;
    Tree(const vector<double>& v,bool mx):maximum(mx) {
        n=1;while(n<(int)v.size())n*=2;
        a.assign(n*2,mx?-numeric_limits<double>::infinity():numeric_limits<double>::infinity());
        for(int i=0;i<(int)v.size();i++)if(isfinite(v[i]))a[n+i]=v[i];
        for(int i=n-1;i;i--)a[i]=mx?max(a[i*2],a[i*2+1]):min(a[i*2],a[i*2+1]);
    }
    int first(int l,int r,double threshold,int node=1,int left=0,int right=-1)const {
        if(right<0)right=n-1;
        if(right<l||left>r||(maximum?a[node]<threshold:a[node]>threshold))return -1;
        if(left==right)return left;
        int mid=(left+right)/2;int hit=first(l,r,threshold,node*2,left,mid);
        return hit>=0?hit:first(l,r,threshold,node*2+1,mid+1,right);
    }
};
struct Risk {double tp,sl;int hold;int minimumHold=0;double stateTarget=numeric_limits<double>::quiet_NaN();};
struct Stats {
    int n=0,w=0,l=0,z=0,unknown=0,liq=0,starved=0,late=0,markers=0,expired=0;
    double net=0,fee=0,pos=0,neg=0,peak=0,dd=0,best=0,worst=0,bank=2500;
    void add(double p,double f) {n++;net+=p;fee+=f;bank+=p;if(p>1e-9){w++;pos+=p;}else if(p < -1e-9){l++;neg-=p;}else z++;peak=max(peak,net);dd=max(dd,peak-net);best=max(best,p);worst=min(worst,p);}
};
struct Trade {int signal,entry,exit,d;double ep,xp,pnl,fee;string reason;bool unknown;};
struct Engine {
    vector<Row> m;vector<vector<Run>> signals;vector<int> bad,markers;vector<int> foldStart,foldEnd;
    Tree midMin,midMax,lowMin,highMax;
    unique_ptr<Tree> stateMin,stateMax;
    double margin=2000,lev=550,buffer=.000476041580087574,rate=15000,position=814.598,reference=100000,winFee=.02;
    double adverseRaw=0; // optional research-only error of the fill-price proxy, per leg
    int entryDelay=5,entrySeen=1,closeDelay=9,closeSeen=2,cooldown=30,ageLimit=6000;
    vector<array<int,4>> latencyPool;uint64_t seed=20261021;
    static uint64_t mix(uint64_t x){x+=0x9e3779b97f4a7c15ULL;x=(x^(x>>30))*0xbf58476d1ce4e5b9ULL;x=(x^(x>>27))*0x94d049bb133111ebULL;return x^(x>>31);}
    void readLatency(const string& path){ifstream f(path);if(!f)throw runtime_error("LATENCY_POOL_OPEN");string line;while(getline(f,line)){if(line.empty())continue;istringstream ss(line);array<int,4> q{};ss>>q[0]>>q[1]>>q[2]>>q[3];if(!ss||q[0]<1||q[2]<1)throw runtime_error("LATENCY_POOL_ROW");latencyPool.push_back(q);}if(latencyPool.empty())throw runtime_error("LATENCY_POOL_EMPTY");}
    void readState(const string& path){ifstream f(path,ios::binary|ios::ate);if(!f)throw runtime_error("EXIT_STATE_OPEN");auto bytes=f.tellg();if(bytes!=streamoff(m.size()*sizeof(double)))throw runtime_error("EXIT_STATE_LENGTH");vector<double> v(m.size());f.seekg(0);f.read((char*)v.data(),bytes);stateMin=make_unique<Tree>(v,false);stateMax=make_unique<Tree>(v,true);}
    static vector<Row> readMarket(const string& path) {
        ifstream f(path,ios::binary|ios::ate);if(!f)throw runtime_error("MARKET_OPEN");auto bytes=f.tellg();
        if(bytes%sizeof(Row))throw runtime_error("MARKET_LAYOUT");
        vector<Row> v((size_t)bytes/sizeof(Row));f.seekg(0);f.read((char*)v.data(),bytes);return v;
    }
    static vector<double> col(const vector<Row>& m,int k) {vector<double> r;for(auto&v:m)r.push_back(k==0?v.px:k==1?v.lo:v.hi);return r;}
    Engine(const string& market,const string& sig):m(readMarket(market)),midMin(col(m,0),false),midMax(col(m,0),true),lowMin(col(m,1),false),highMax(col(m,2),true) {
        ifstream f(sig,ios::binary);int32_t n=0;f.read((char*)&n,4);if(n<1||n>100000)throw runtime_error("SIGNAL_HEADER");signals.resize(n);
        for(auto& s:signals){int32_t count;f.read((char*)&count,4);if(count<0||count>1000000)throw runtime_error("RUN_COUNT");s.resize(count);f.read((char*)s.data(),count*sizeof(Run));}
        foldStart.assign(5,-1);foldEnd.assign(5,-1);
        for(int i=0;i<(int)m.size();i++){int k=(int)m[i].fold;if(k<0||k>4)throw runtime_error("FOLD");if(foldStart[k]<0)foldStart[k]=i;foldEnd[k]=i;}
        rebuildBad();
    }
    void rebuildBad(){bad.assign(m.size()+1,0);markers.assign(m.size()+1,0);for(int i=0;i<(int)m.size();i++){bad[i+1]=bad[i]+(!isfinite(m[i].px)||m[i].age>ageLimit);markers[i+1]=markers[i]+(m[i].gap!=0);}}
    pair<double,double> payout(double ep,double xp,int d)const {
        long long a=llround(ep),p=llround(xp),threshold=a/50000;
        long long eff=d==1?(p>a+threshold?p-threshold:p>a?a:p):(p+threshold<a?p+threshold:p<a?a:p);
        double v=double((eff-a)*d)/a;double gross=margin*lev*v;
        if(gross<=0)return {max(gross,-margin),0};
        double factor=.9/(1+1/(v*rate)+reference/(1e6*v*position));
        double adjusted=gross*factor,fee=adjusted*winFee;return {adjusted-fee,fee};
    }
    Stats run(int entryId,Risk risk,int fold,vector<Trade>*ledger=nullptr)const {
        int begin=fold<0?0:foldStart[fold],end=fold<0?(int)m.size()-1:foldEnd[fold];Stats s;
        if(begin<0)return s;
        int freeAt=begin;int lastSignal=end-risk.hold-entryDelay-closeDelay-closeSeen-2;
        if(!latencyPool.empty()){
            int horizon=50;
            for(const auto& q:latencyPool)if(q[0]<=20)horizon=max(horizon,q[0]+risk.hold+q[2]+q[3]+2);
            lastSignal=min(lastSignal,end-horizon);
        }
        for(const auto& run:signals.at(entryId)) {
            if(run.end<freeAt||run.end<begin)continue;
            if(run.start>lastSignal)break;
            int i=max({run.start,freeAt,begin});
            while(i<=run.end&&i<=lastSignal) {
                if(s.bank+1e-8<margin){s.starved++;return s;}
                array<int,4> delay{entryDelay,entrySeen,closeDelay,closeSeen};
                if(!latencyPool.empty())delay=latencyPool[mix(uint64_t(m[i].t)^seed)%latencyPool.size()];
                if(delay[0]>20){s.expired++;freeAt=i+50;i=max(i+1,freeAt);continue;}
                int ei=i+delay[0],d=run.d;int decision=ei+risk.hold;
                double ep=m[ei].px+d*adverseRaw,xp=ep,net=0,fee=0;int ex=ei;bool unknown=false;string reason;
                if(ei>run.end)s.late++;
                if(!isfinite(ep)||ep<=0||m[ei].age>ageLimit){unknown=true;reason="unknown_entry";net=-margin;}
                else {
                    int from=ei+max(1,delay[1]),through=ei+risk.hold;
                    int tpFrom=max(from,ei+risk.minimumHold);
                    int tp=isfinite(risk.stateTarget)?(d==1?stateMax->first(tpFrom,through,risk.stateTarget):stateMin->first(tpFrom,through,risk.stateTarget)):
                        (d==1?midMax.first(tpFrom,through,ep*(1+risk.tp/10000)):midMin.first(tpFrom,through,ep*(1-risk.tp/10000)));
                    int sl=d==1?lowMin.first(from,through,ep*(1-risk.sl/10000)):highMax.first(from,through,ep*(1+risk.sl/10000));
                    reason="timeout";
                    if(tp>=0&&tp<=decision){decision=tp;reason=isfinite(risk.stateTarget)?"state_target":"tp";}
                    if(sl>=0&&sl<=decision){decision=sl;reason="sl";}
                    ex=decision+delay[2];
                    double bust=ep*(lev-d)/(lev*(1-d*buffer));
                    int liq=d==1?lowMin.first(ei+1,ex,bust):highMax.first(ei+1,ex,bust);
                    if(liq>=0){ex=liq;xp=d==1?m[ex].lo:m[ex].hi;net=-margin;reason="liquidation";s.liq++;}
                    else {xp=m[ex].px-d*adverseRaw;auto p=payout(ep,xp,d);net=p.first;fee=p.second;}
                    s.markers+=markers[ex+1]-markers[ei];
                    if(bad[ex+1]-bad[ei]>0){unknown=true;reason="unknown_path";net=-margin;fee=0;}
                }
                if(unknown)s.unknown++;
                s.add(net,fee);
                if(ledger)ledger->push_back({i,ei,ex,d,ep,xp,net,fee,reason,unknown});
                freeAt=ex+delay[3]+cooldown;i=max(i+1,freeAt);
            }
        }
        return s;
    }
};
Risk riskAt(int id) {array<double,8> tp{.25,.5,1,2,3,4,6,8};array<double,5> sl{2,4,6,8,12};array<int,5> hold{30,60,120,300,600};return {tp.at(id/25),sl.at((id/5)%5),hold.at(id%5)};}
Risk tightRiskAt(int id) {array<double,8> tp{.25,.5,.75,1,1.5,2,3,4};array<double,8> sl{.25,.5,1,1.5,2,3,4,6};array<int,4> hold{30,60,120,300};int minimum=(id%2)*10;id/=2;return {tp.at(id/32),sl.at((id/4)%8),hold.at(id%4),minimum};}
void header(ostream& o){o<<"entry,risk,fold,n,wins,losses,zero,wr,net,fee,pf,drawdown,best,worst,without_best,unknown,liquidations,capital_blocked,bank,late,capture_markers,expired\n";}
void emit(ostream&o,int e,int r,int f,const Stats&s){o<<e<<','<<r<<','<<f<<','<<s.n<<','<<s.w<<','<<s.l<<','<<s.z<<','<<(s.n?double(s.w)/s.n:0)<<','<<s.net<<','<<s.fee<<','<<(s.neg?s.pos/s.neg:999999)<<','<<s.dd<<','<<s.best<<','<<s.worst<<','<<s.net-s.best<<','<<s.unknown<<','<<s.liq<<','<<s.starved<<','<<s.bank<<','<<s.late<<','<<s.markers<<','<<s.expired<<'\n';}
int main(int argc,char**argv){try {
    if(argc<5)throw runtime_error("usage: market signals output train|cases [casesfile] [positionMultiplier] [buffer]");
    Engine eng(argv[1],argv[2]);if(argc>6)eng.position=stod(argv[6]);if(argc>7)eng.buffer=stod(argv[7]);
    bool tight=argc>8&&string(argv[8])=="tight";
    vector<Risk> customRisks;
    if(argc>8&&string(argv[8]).rfind("grid:",0)==0){
        ifstream rf(string(argv[8]).substr(5));if(!rf)throw runtime_error("RISK_GRID_OPEN");
        string line;while(getline(rf,line)){if(line.empty())continue;istringstream ss(line);Risk r{};ss>>r.tp>>r.sl>>r.hold>>r.minimumHold;
            if(!ss||r.tp<=0||r.sl<=0||r.hold<1||r.minimumHold<0||r.minimumHold>r.hold)throw runtime_error("RISK_GRID_ROW");
            double target;if(ss>>target)r.stateTarget=target;
            customRisks.push_back(r);}
        if(customRisks.empty())throw runtime_error("RISK_GRID_EMPTY");
    }
    auto riskFor=[&](int i){return !customRisks.empty()?customRisks.at(i):tight?tightRiskAt(i):riskAt(i);};
    int riskCount=customRisks.empty()?(tight?512:200):int(customRisks.size());
    if(argc>9)eng.margin=stod(argv[9]);
    if(argc>10)eng.lev=stod(argv[10]);
    if(argc>11)eng.entryDelay=stoi(argv[11]);
    if(argc>12)eng.closeDelay=stoi(argv[12]);
    if(argc>13){eng.ageLimit=stoi(argv[13]);eng.rebuildBad();}
    if(argc>14&&string(argv[14])!="-")eng.readLatency(argv[14]);
    if(argc>15)eng.seed=stoull(argv[15]);
    if(argc>17){eng.adverseRaw=stod(argv[17]);if(eng.adverseRaw<0)throw runtime_error("NEGATIVE_ADVERSE_RAW");}
    if(argc>18)eng.readState(argv[18]);
    for(const auto& r:customRisks)if(isfinite(r.stateTarget)&&!eng.stateMin)throw runtime_error("MISSING_EXIT_STATE");
    ofstream out(argv[3]);out<<setprecision(15);header(out);string mode=argv[4];
    if(mode=="train") {
        for(int e=0;e<(int)eng.signals.size();e++) {
            for(int r=0;r<riskCount;r++)emit(out,e,r,0,eng.run(e,riskFor(r),0));
            if(e%25==0)cerr<<"TRAIN "<<e<<'/'<<eng.signals.size()<<'\n';
        }
    } else if(mode=="cases") {
        if(argc<6)throw runtime_error("MISSING_CASES");
        ifstream f(argv[5]);string line;
        while(getline(f,line)) {
            if(line.empty())continue;
            istringstream ss(line);int e,r;ss>>e>>r;
            for(int fold:{0,1,2,3,4,-1})emit(out,e,r,fold,eng.run(e,riskFor(r),fold));
        }
    } else if(mode=="ledger") {
        if(argc<6)throw runtime_error("MISSING_CASES");
        ifstream f(argv[5]);int e,r;f>>e>>r;vector<Trade> trades;
        int ledgerFold=argc>16?stoi(argv[16]):-1;
        eng.run(e,riskFor(r),ledgerFold,&trades);out.close();ofstream l(argv[3]);l<<setprecision(15)<<"signal,entry,exit,side,entry_raw,exit_raw,net,fee,reason,unknown\n";
        for(auto& t:trades)l<<int64_t(eng.m[t.signal].t*1000)<<','<<int64_t(eng.m[t.entry].t*1000)<<','<<int64_t(eng.m[t.exit].t*1000)<<','<<t.d<<','<<t.ep<<','<<t.xp<<','<<t.pnl<<','<<t.fee<<','<<t.reason<<','<<t.unknown<<'\n';
    } else throw runtime_error("UNKNOWN_MODE");
    return 0;
}catch(const exception&e){cerr<<e.what()<<'\n';return 2;}}
