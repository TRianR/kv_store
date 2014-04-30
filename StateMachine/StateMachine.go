package sm

type StateMachine interface {
        Get(string) string
        Set(string, string)
}

type mcstruct struct {
        constant int
        m map[string]string
}

func (mc *mcstruct) Get(key string) string {
        if (mc.m[key] != ""){
                return mc.m[key]
        }
        return ""
}
func (mc *mcstruct) Set(key string, value string) {
        mc.m[key] = value
        
}
func NewStatemachine (id int) StateMachine {
        newSM := mcstruct{id,map[string]string{
            "KnockKnock":  "Who's There?",
            "Honk"      : "Honk",
            "Donkey"    : "Monkey",
            "Honey"     : "Money",
        }}
        return &newSM
}
