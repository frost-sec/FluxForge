package objects

import (
 "context"
 "testing"
 "fluxforge-control/internal/domain"
)
type memoryRepo struct{ values map[string]Upload }
func(m *memoryRepo)Create(_ context.Context,u Upload)error{m.values[u.ID]=u;return nil}
func(m *memoryRepo)Find(_ context.Context,id string)(Upload,error){u,ok:=m.values[id];if !ok{return u,domain.E(domain.NotFound,"object missing")};return u,nil}
func(m *memoryRepo)Commit(_ context.Context,id,v string)(domain.ObjectRef,error){u:=m.values[id];if !u.Committed{u.Committed=true;u.Ref.VersionID=v;m.values[id]=u};return u.Ref,nil}
type memoryBackend struct{ version string; observedSize int64 }
func(b *memoryBackend)UploadURL(context.Context,string,bool)(string,error){return "http://minio/upload",nil}
func(b *memoryBackend)UploadedVersion(_ context.Context,_ string,n int64)(string,error){b.observedSize=n;return b.version,nil}
func(b *memoryBackend)DownloadURL(_ context.Context,r domain.ObjectRef,_ bool)(string,error){return "http://minio/"+r.Key+"?versionId="+r.VersionID,nil}
func TestCommitPinsVersionAndNormalizesReference(t *testing.T){
 ctx:=context.Background();repo:=&memoryRepo{map[string]Upload{}};backend:=&memoryBackend{version:"v-one"};s:=New(repo,backend,"test-bucket")
 u,_,e:=s.Begin(ctx,"RUN_INPUT",10,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","application/json",true);if e!=nil{t.Fatal(e)}
 if _,e=s.Resolve(ctx,u.Ref);e==nil{t.Fatal("uncommitted object accepted")}
 first,e:=s.Commit(ctx,u.ID);if e!=nil{t.Fatal(e)};backend.version="v-two";second,e:=s.Commit(ctx,u.ID);if e!=nil{t.Fatal(e)};if first.VersionID!="v-one"||second.VersionID!="v-one"{t.Fatal("commit must not advance to overwritten version")}
 hostile:=domain.ObjectRef{ID:u.ID,Bucket:"other",Key:"attacker",VersionID:"v-two"};resolved,e:=s.Resolve(ctx,hostile);if e!=nil{t.Fatal(e)};if resolved.Bucket!="test-bucket"||resolved.VersionID!="v-one"{t.Fatal("external storage location trusted")}
 hostile.SHA256="wrong";if _,e=s.Resolve(ctx,hostile);e==nil{t.Fatal("conflicting digest accepted")}
}
func TestInvalidUploads(t *testing.T){s:=New(&memoryRepo{map[string]Upload{}},&memoryBackend{},"test");for _,purpose:=range []string{"output","","../secret"}{if _,_,e:=s.Begin(context.Background(),purpose,1,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","",false);e==nil{t.Fatal("invalid purpose accepted")}}}
