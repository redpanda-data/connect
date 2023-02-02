package svcdiscover

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"gopkg.in/natefinch/lumberjack.v2"
)

func init() {

}

type Nacos struct{}

func NewNacos() ServiceDiscoverReg {
	return &Nacos{}
}

func (n *Nacos) RegisterInstance(conf Config, httpAddr string, logger log.Modular) error {

	if conf.Enabled {
		if conf.Nacos.ServerAddr == "" {
			return nil
		}
		if conf.Nacos.ServerPort < 0 || conf.Nacos.ServiceName == "" {
			return fmt.Errorf("nacos server_port must be gt 0, and service_name must be not empty")
		}

		serverIp := conf.Nacos.ServerAddr
		serverPort := conf.Nacos.ServerPort
		serviceName := conf.Nacos.ServiceName
		namespaceId := conf.Nacos.Namespace

		sc := []constant.ServerConfig{
			*constant.NewServerConfig(serverIp, uint64(serverPort)),
		}

		cc := *constant.NewClientConfig(
			constant.WithNamespaceId(namespaceId),
			constant.WithTimeoutMs(10000),
			constant.WithBeatInterval(5000),
			constant.WithNotLoadCacheAtStart(true),
			constant.WithLogDir("./logs/nacos/log"),
			constant.WithCacheDir("/tmp/nacos/cache"),
			constant.WithLogLevel("warn"),
			constant.WithLogRollingConfig(&lumberjack.Logger{MaxSize: 10, MaxAge: 3}),
		)

		client, err := clients.NewNamingClient(
			vo.NacosClientParam{
				ClientConfig:  &cc,
				ServerConfigs: sc,
			},
		)
		if err != nil {
			return err

		}
		regIp := conf.Nacos.RegistryIp
		if regIp == "" {
			ipv4, err := GetLocalIPv4()
			if err != nil {
				return err
			}
			regIp = ipv4.To4().String()
		}

		svcPort := 4195
		address := strings.Split(httpAddr, ":")
		if len(address) == 2 {
			if p, err := strconv.Atoi(address[1]); err == nil {
				svcPort = p
			}
		}
		success, err := client.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          regIp,
			Port:        uint64(svcPort),
			ServiceName: serviceName,
			Weight:      10,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
			//Metadata:    map[string]string{"idc": "shanghai"},
		})
		if err != nil {
			return err
		}
		logger.Infof("nacos server resp: %v, register %v@%v on ns %v to %v:%v\n", success, serviceName, regIp, namespaceId, serverIp, serverPort)
	}
	return nil
}

func NacosSpec() docs.FieldSpec {
	return docs.FieldObject("nacos", "nacos configuration").WithChildren(
		docs.FieldString("server_addr", "The address of nacos server.").HasDefault(""),
		docs.FieldInt("server_port", "The port of nacos server.").HasDefault(8848),
		docs.FieldString("namespace", "The namesapce to register to.").HasDefault("public"),
		docs.FieldString("service_name", "The local service name to register to.").HasDefault("benthos"),
		docs.FieldString("registry_ip", "The local ip addr to register to.").HasDefault(""))
}
